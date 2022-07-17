package com.github.karlnicholas.merchloan.servicerequest;

import com.github.karlnicholas.merchloan.sqlutil.SqlInitialization;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

public class ServiceRequestApplication {

    public static void main(String[] args) throws LifecycleException {
        Tomcat tomcat = new Tomcat();
        tomcat.setBaseDir("temp");
        tomcat.setPort(8080);

        String contextPath = "";
        String docBase = new File(".").getAbsolutePath();

        Context context = tomcat.addContext(contextPath, docBase);

        class SampleServlet extends HttpServlet {

            @Override
            protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                    throws ServletException, IOException {
                PrintWriter writer = resp.getWriter();

                writer.println("<html><title>Welcome</title><body>");
                writer.println("<h1>Have a Great Day!</h1>");
                writer.println("</body></html>");
            }
        }

        String servletName = "SampleServlet";
        String urlPattern = "/aa";

        tomcat.addServlet(contextPath, servletName, new SampleServlet());
        context.addServletMappingDecoded(urlPattern, servletName);

        tomcat.start();
        tomcat.getServer().await();
    }

    public void initialize() throws SQLException, IOException, ActiveMQException {
        DataSource dataSource;
        try(Connection con = dataSource.getConnection()) {
            SqlInitialization.initialize(con, ServiceRequestApplication.class.getResourceAsStream("/sql/schema.sql"));
        }
    }
}
