package com.github.karlnicholas.merchloan.jms.utils;

import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl;
import org.apache.activemq.artemis.api.core.management.ObjectNameBuilder;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class CleanupMerchloan {
    private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://localhost:1099/jmxrmi";

    public static void main(String[] args) throws Exception {
        new CleanupMerchloan().run();
    }

    private void run() throws Exception {

        ObjectNameBuilder objectNameBuilder = ObjectNameBuilder.create("org.apache.activemq.artemis", "0.0.0.0", true);
        ObjectName serverObjectName = objectNameBuilder.getActiveMQServerObjectName();

        try (JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(CleanupMerchloan.JMX_URL))) {
            MBeanServerConnection mbsc = connector.getMBeanServerConnection();

            ActiveMQServerControl serverControl = MBeanServerInvocationHandler.newProxyInstance(mbsc, serverObjectName, ActiveMQServerControl.class, false);

            for (String name : serverControl.getAddressNames()) {
                System.out.println(name);
                if (name.contains("query") || name.contains("account") || name.contains("statement") || name.contains("service") || name.contains("businessdate")) {
                    System.out.println(name);
                    serverControl.deleteAddress(name);
                }
            }

        }
    }

}
