
import os
import threading
from collections import defaultdict

import yaml
from flask import Flask, abort, request
from elijah.provisioning.synthesis_client import Client, Protocol

app = Flask(__name__)

network_lock = threading.Lock()


class OutOfNetworksError(Exception):
    """Raised when no more networks are available."""


class MissingConfigError(Exception):
    """Raised when the configuration is wrong."""


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
        for _, network in app.config['CLOUDLET_STATE'].items()
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
    app.config['CLOUDLET_STATE'][user_id] = {
        'network': selected_network,
        'apps': {},
    }
    return app.config['CLOUDLET_STATE'][user_id]


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
        del app.config['CLOUDLET_STATE'][user_id]


def get_user_network(user_id, create=False):
    """Return the tenant network for the user to use."""
    network = app.config['CLOUDLET_STATE'].get(user_id)
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
            for _, network in app.config['CLOUDLET_STATE'].items()
        )
    sorted_cloudlets = sorted(found_cloudlets.keys(), key=_cloudlet_sorter)
    if not sorted_cloudlets:
        return None
    data = found_cloudlets[sorted_cloudlets[0]]
    data['name'] = sorted_cloudlets[0]
    return data


def launch_vm(cloudlet, network, overlay_path):
    """Launch the VM on the `cloudlet` connected to network."""
    cloudlet_ip = cloudlet['ip']
    cloudlet_port = cloudlet.get('port', 8021)
    cloudlet_interface = cloudlet['networks'][network]
    cloudlet_options = dict()
    cloudlet_options[Protocol.SYNTHESIS_OPTION_DISPLAY_VNC] = False
    cloudlet_options[Protocol.SYNTHESIS_OPTION_EARLY_START] = False
    cloudlet_options[Protocol.SYNTHESIS_OPTION_INTERFACE] = (
        cloudlet_interface)
    cloudlet_client = Client(
        cloudlet_ip, cloudlet_port,
        overlay_file=overlay_path,
        app_function=lambda: None, synthesis_option=cloudlet_options)
    cloudlet_client.provisioning()
    return cloudlet_client


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
            app.logger.info(
                "starting app '%s' for user '%s' on cloudlet server '%s' "
                "connected to network '%s'",
                app_id, user_id, selected_cloudlet['name'], network['network'])
            cloudlet_client = launch_vm(
                selected_cloudlet, network['network'], overlay_path)
            app.logger.info(
                "started app '%s' for user '%s' on cloudlet server '%s' "
                "connected to network '%s'",
                app_id, user_id, selected_cloudlet['name'], network['network'])
            network['apps'][app_id] = {
                'cloudlet': selected_cloudlet,
                'client': cloudlet_client,
            }
            return "Success"
        elif action == "delete":
            network, user_app = get_network_and_app(user_id, app_id)
            user_app['client'].terminate()
            atomic_network_release(user_id, app_id)
            return "Success"
        else:
            abort(400)
    else:
        network, user_app = get_network_and_app(user_id, app_id)
        return "Found"


def run_server(config_file=None, overlays_path=None, debug=False):
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
        config = yaml.load(stream.read())
    if not os.path.exists(overlays_path):
        os.makedirs(overlays_path)
    app.config['CLOUDLET_CONFIG'] = config
    app.config['CLOUDLET_STATE'] = {}
    app.config['OVERLAYS_PATH'] = overlays_path
    app.run(debug=debug)


if __name__ == '__main__':
    main()
