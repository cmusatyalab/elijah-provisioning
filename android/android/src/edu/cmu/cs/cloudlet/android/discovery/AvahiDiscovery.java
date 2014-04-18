package edu.cmu.cs.cloudlet.android.discovery;

import java.io.IOException;
import java.net.Inet4Address;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceEvent;
import javax.jmdns.ServiceInfo;
import javax.jmdns.ServiceListener;

import android.app.Activity;
import android.widget.ArrayAdapter;

public class AvahiDiscovery {
	android.net.wifi.WifiManager.MulticastLock lock;

	private ArrayAdapter<CloudletDevice> listAdapter = null;
	private Activity activity = null;

	public AvahiDiscovery(Activity activity, ArrayAdapter<CloudletDevice> listAdapter) {
		this.activity = activity;
		this.listAdapter = listAdapter;
	}

	private String type = "_cloudlet._udp.local.";
	private JmDNS jmdns = null;
	private ServiceListener listener = null;
	private ServiceInfo serviceInfo;

	public void start() {
		android.net.wifi.WifiManager wifi = (android.net.wifi.WifiManager) activity
				.getSystemService(android.content.Context.WIFI_SERVICE);
		lock = wifi.createMulticastLock("wifimulticastlock");
		lock.setReferenceCounted(true);
		lock.acquire();
		try {
			jmdns = JmDNS.create();
			jmdns.addServiceListener(type, listener = new ServiceListener() {

				@Override
				public void serviceResolved(final ServiceEvent ev) {
					activity.runOnUiThread(new Runnable() {
						public void run() {
							ServiceInfo si = ev.getInfo();
							Inet4Address[] addresses = si.getInet4Addresses();
							for(int i = 0; i < addresses.length; i++){
								String addr = addresses[i].getHostAddress();
								CloudletDevice d = new CloudletDevice(addr, ev.getInfo().getPort());
								int position = listAdapter.getPosition(d);
								if (position >= 0) {
									// Device already in the list, re-set new value at same
									// position
									listAdapter.remove(d);
									listAdapter.insert(d, position);
									listAdapter.notifyDataSetChanged();
								} else {
									listAdapter.add(d);
									listAdapter.notifyDataSetChanged();
								}
							}
						}
					});
				}

				@Override
				public void serviceRemoved(final ServiceEvent ev) {
					activity.runOnUiThread(new Runnable() {
						public void run() {
							ServiceInfo si = ev.getInfo();
							Inet4Address[] addresses = si.getInet4Addresses();
							for(int i = 0; i < addresses.length; i++){
								String addr = addresses[i].getHostAddress();
								CloudletDevice d = new CloudletDevice(addr, ev.getInfo().getPort());
								listAdapter.remove(d);
								listAdapter.notifyDataSetChanged();							
							}
						}
					});
				}

				@Override
				public void serviceAdded(ServiceEvent event) {
					// Required to force serviceResolved to be called again
					// (after the first search)
					jmdns.requestServiceInfo(event.getType(), event.getName(), 1);
				}
			});
			// serviceInfo = ServiceInfo.create("_cloudlet._tcp.local.",
			// "AndroidTest", 8000,
			// "plain test service from android");
			// jmdns.registerService(serviceInfo);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}

	public void close() {
		if (jmdns != null) {
			if (listener != null) {
				jmdns.removeServiceListener(type, listener);
				listener = null;
			}
			jmdns.unregisterAllServices();
			try {
				jmdns.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			jmdns = null;
		}
		lock.release();
	}
}