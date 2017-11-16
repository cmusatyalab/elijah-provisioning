
import os
import random
import threading
from collections import defaultdict
from subprocess import check_call

import yaml
from flask import Flask, abort, jsonify, request
from elijah.gateway.lease_parser import Leases
from elijah.provisioning.synthesis_client import Client, Protocol

app = Flask(__name__)

network_lock = threading.Lock()


class OutOfNetworksError(Exception):
    """Raised when no more networks are available."""


class MissingConfigError(Exception):
    """Raised when the configuration is wrong."""


def random_mac():
    """Generate a random MAC address."""
    return "52:54:00:%02x:%02x:%02x" % (
        random.randint(0, 255),
        random.randint(0, 255),
        random.randint(0, 255),
        )


def synchronized(lock):
    """
    Decorator that prevents multiple threads to use the same function
    at the same time.
    """
    def wrap(f):
        def newFunction(*args, **kw):
            lock.acquire()
            try:
                return f(*args, **kw)
            finally:
                lock.release()
        return newFunction
    return wrap


def get_app_state():
    """Return the state of the application."""
    return app.config['CLOUDLET_STATE']


def get_all_networks():
    """Return all the networks available.

    This uses the networks defined in the config and the network must also
    exist on at least one cloudlet.
    """
    found_networks = set()
    for cloudlet, data in app.config['CLOUDLET_CONFIG']['cloudlets'].items():
        for network, interface in data['networks'].items():
            found_networks.add(network)
    return {
        network: interface
        for network, interface in (
            app.config['CLOUDLET_CONFIG']['networks'].items())
        if network in found_networks
    }


@synchronized(network_lock)
def atomic_network_allocate(user_id):
    """Reserve a free network for `user_id`."""
    used_networks = [
        network['network']
        for _, network in get_app_state().items()
    ]
    available_networks = [
        network
        for network, interface in get_all_networks().items()
        if network not in used_networks
    ]
    if not available_networks:
        app.logger.error(
            "no available networks for new user '%s'", user_id)
        raise OutOfNetworksError()
    selected_network = available_networks[0]
    app.logger.info(
        "selected new network '%s' for user '%s'", selected_network, user_id)
    get_app_state()[user_id] = {
        'network': selected_network,
        'apps': {},
    }
    return get_app_state()[user_id]


@synchronized(network_lock)
def atomic_network_release(user_id, app_id):
    """Release the network `user_id` and `app_id` is using."""
    network = get_user_network(user_id, create=False)
    if not network:
        app.logger.info(
            "no need to release network for user '%s', currently no "
            "network allocated", user_id)
        return
    network['apps'].pop(app_id, None)
    if not network['apps']:
        app.logger.info(
            "releasing network '%s' for user '%s', no apps running",
            network['network'], user_id)
        del get_app_state()[user_id]


def get_user_network(user_id, create=False):
    """Return the tenant network for the user to use."""
    network = get_app_state().get(user_id)
    if network is None and create:
        return atomic_network_allocate(user_id)
    return network


def get_network_and_app(user_id, app_id):
    """Get the network and the app.

    Aborts with 404 if cannot be found.
    """
    network = get_user_network(user_id)
    if not network:
        abort(404)
    app = network['apps'].get(app_id)
    if not app:
        abort(404)
    return network, app


def select_cloudlet(network):
    """Select the best cloudlet to start a new VM on."""
    found_cloudlets = {}
    for cloudlet, data in app.config['CLOUDLET_CONFIG']['cloudlets'].items():
        for connected_network, interface in data['networks'].items():
            if connected_network == network:
                found_cloudlets[cloudlet] = data
                break
    # Pick the cloudlet that is running the lease number of apps.
    def _cloudlet_sorter(cloudlet):
        return sum(
            len(network['apps'])
            for _, network in get_app_state().items()
        )
    sorted_cloudlets = sorted(found_cloudlets.keys(), key=_cloudlet_sorter)
    if not sorted_cloudlets:
        return None
    data = found_cloudlets[sorted_cloudlets[0]]
    data['name'] = sorted_cloudlets[0]
    return data


@synchronized(network_lock)
def start_network(network):
    """Start the networking for the network."""
    started = network.get('open', False)
    if started:
        return
    app.logger.info("starting network '%s'", network['network'])
    config = app.config['CLOUDLET_CONFIG']
    net_info = config['networks'][network['network']]
    check_call(
        ['cloudlet-add-vlan', net_info['interface'], str(net_info['vid'])])
    network['open'] = True


@synchronized(network_lock)
def stop_network(network):
    """Stop the networking for the network."""
    started = network.get('open', False)
    if not started:
        return
    app.logger.info("stopping network '%s'", network['network'])
    config = app.config['CLOUDLET_CONFIG']
    net_info = config['networks'][network['network']]
    check_call(['cloudlet-delete-vlan', str(net_info['vid'])])
    del network['open']


def launch_vm(cloudlet, network, mac, overlay_path):
    """Launch the VM on the `cloudlet` connected to network."""
    cloudlet_ip = cloudlet['ip']
    cloudlet_port = cloudlet.get('port', 8021)
    cloudlet_interface = cloudlet['networks'][network]
    cloudlet_options = dict()
    cloudlet_options[Protocol.SYNTHESIS_OPTION_DISPLAY_VNC] = False
    cloudlet_options[Protocol.SYNTHESIS_OPTION_EARLY_START] = False
    cloudlet_options[Protocol.SYNTHESIS_OPTION_INTERFACE] = (
        cloudlet_interface)
    cloudlet_options[Protocol.SYNTHESIS_OPTION_INTERFACE_MAC] = mac
    cloudlet_client = Client(
        cloudlet_ip, cloudlet_port,
        overlay_file=overlay_path,
        app_function=lambda: None, synthesis_option=cloudlet_options)
    cloudlet_client.provisioning()
    return cloudlet_client


def wait_for_ip(network, mac):
    """Wait for the IP address for `mac` on `network`."""
    config = app.config['CLOUDLET_CONFIG']
    net_info = config['networks'][network]
    leases_path = os.path.join(
        '/var/lib/cloudlet/dnsmasq/vlan-%d.leases' % net_info['vid'])
    for _ in range(4 * 30):  # 30 seconds.
        leases = Leases(leases_path)
        for entry in leases.entries():
            if entry.mac.lower() == mac.lower():
                return entry.ip
        time.sleep(0.25)
    return None


@app.route('/', methods=['GET', 'POST'])
def index():
    """Endpoint to get app information and to start a new application."""
    user_id = request.values.get("user_id")
    app_id = request.values.get("app_id")
    if request.method == 'POST':
        action = request.values.get("action")
        if action == "create":
            try:
                network = get_user_network(user_id, create=True)
            except OutOfNetworksError:
                abort(400)
            user_app = network['apps'].get(app_id)
            if user_app:
                app.logger.error(
                    "app '%s' for user '%s' is already running in cloudlet",
                    app_id, user_id)
                abort(400)
            # Starting a new application on the selected network. Find the
            # best cloudlet server to run the application on.
            selected_cloudlet = select_cloudlet(network['network'])
            if not selected_cloudlet:
                app.logger.error(
                    "failed to select a cloudlet server to run app '%s' "
                    "for user '%s'", app_id, user_id)
                abort(400)
            # Determine if a file was provided on the request to create.
            overlay_file = request.files.get('overlay')
            overlay_path = os.path.join(
                app.config['OVERLAYS_PATH'], '%s_%s.overlay' % (
                    user_id, app_id))
            if overlay_file:
                overlay_file.save(overlay_path)
            else:
                abort(400)
            start_network(network)
            interface_mac = random_mac()
            app.logger.info(
                "starting app '%s' for user '%s' on cloudlet server '%s' "
                "connected to network '%s' with MAC '%s'",
                app_id, user_id, selected_cloudlet['name'], network['network'],
                interface_mac)
            cloudlet_client = launch_vm(
                selected_cloudlet, network['network'],
                interface_mac, overlay_path)
            app.logger.info(
                "started app '%s' for user '%s' on cloudlet server '%s' "
                "connected to network '%s' with MAC '%s'",
                app_id, user_id, selected_cloudlet['name'], network['network'],
                interface_mac)
            app.logger.info(
                "waiting for app '%s' for user '%s' on cloudlet server '%s' "
                "connected to network '%s' with MAC '%s' to get an IP address",
                app_id, user_id, selected_cloudlet['name'], network['network'],
                interface_mac)
            vm_ip = wait_for_ip(network['network'], interface_mac)
            if not vm_ip:
                # Never got an IP address, so lets stop the client.
                app.logger.error(
                    "failed waiting for IP address for app '%s' for user '%s' "
                    "on cloudlet server '%s' connected to network '%s' "
                    "with MAC '%s'",
                    app_id, user_id, selected_cloudlet['name'],
                    network['network'], interface_mac)
                cloudlet_client.terminate()
                if get_user_network(user_id) is None:
                    stop_network(network)
                abort(400)
            network['apps'][app_id] = {
                'cloudlet': selected_cloudlet,
                'client': cloudlet_client,
                'mac': interface_mac,
                'ip': vm_ip,
            }
            return "Success"
        elif action == "delete":
            network, user_app = get_network_and_app(user_id, app_id)
            user_app['client'].terminate()
            atomic_network_release(user_id, app_id)
            if get_user_network(user_id) is None:
                stop_network(network)
            return "Success"
        else:
            abort(400)
    else:
        network, user_app = get_network_and_app(user_id, app_id)
        return jsonify({
            'mac': user_app['mac'],
            'ip': vm_ip,
        })


def run_server(
        config_file=None, overlays_path=None, debug=False,
        host=None, port=None):
    """Run the flask server."""
    if not config_file:
        config_file = os.path.join('/etc', 'cloudlet', 'config.yaml')
    if not overlays_path:
        overlays_path = os.path.join(
            '/var', 'lib', 'cloudlet', 'uploads', 'overlays')
    if not os.path.exists(config_file):
        raise MissingConfigError("'%s' doesn't exist." % config_file)
    with open(config_file, 'r') as stream:
        data = stream.read().strip()
        if not data:
            raise MissingConfigError("'%s' is empty." % config_file)
        config = yaml.load(data)
    if not os.path.exists(overlays_path):
        os.makedirs(overlays_path)
    app.config['CLOUDLET_CONFIG'] = config
    app.config['CLOUDLET_STATE'] = {}
    app.config['OVERLAYS_PATH'] = overlays_path
    app.run(debug=debug, host=host, port=port)


if __name__ == '__main__':
    main()
