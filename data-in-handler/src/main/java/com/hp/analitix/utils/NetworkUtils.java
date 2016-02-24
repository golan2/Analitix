package com.hp.analitix.utils;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;


/**
 * Created by golaniz on 18/02/2016.
 */
public class NetworkUtils {

    public static final String SSH_CONNECTION_XML = "ssh-connection.xml";

    public static Session openSshSession() throws JSchException, FileNotFoundException {
        URL resource = ReadFromHdfs.class.getClassLoader().getResource(SSH_CONNECTION_XML);
        if (resource==null) throw new FileNotFoundException("Can't fine ["+SSH_CONNECTION_XML+"] in classpath");
        String sshConnectionFilePath = resource.getPath().substring(1);
        SSHConnectionProperties sshProperties = loadSshConfig(sshConnectionFilePath);
        Session sshSession = SSHConnection.establish(sshProperties);
        sshSession.setPortForwardingL(9000, "localhost", 9000);
        sshSession.setPortForwardingL(50090 , "localhost", 50090);
        sshSession.setPortForwardingL(50091 , "localhost", 50091);
        sshSession.setPortForwardingL(50010 , "localhost", 50010);
        sshSession.setPortForwardingL(50075 , "localhost", 50075);
        sshSession.setPortForwardingL(50020 , "localhost", 50020);
        sshSession.setPortForwardingL(50070 , "localhost", 50070);
        sshSession.setPortForwardingL(50475 , "localhost", 50475);
        sshSession.setPortForwardingL(50470 , "localhost", 50470);
        sshSession.setPortForwardingL(50100 , "localhost", 50100);
        sshSession.setPortForwardingL(50105 , "localhost", 50105);
        sshSession.setPortForwardingL(8485  , "localhost", 8485 );
        sshSession.setPortForwardingL(8480  , "localhost", 8480 );
        sshSession.setPortForwardingL(8481  , "localhost", 8481 );
        return sshSession;
    }

    private static SSHConnectionProperties loadSshConfig(String configFilePath) {
        SSHConnectionProperties sshConnectionProperties = null;
        File file = new File(configFilePath);
        System.out.println("Loading SSH Connection configuration details from " + file.getAbsolutePath() + "...");
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(SSHConnectionProperties.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            sshConnectionProperties = (SSHConnectionProperties) unmarshaller.unmarshal(file);
        } catch (JAXBException e) {
            String originalMsg = e.getMessage();
            String msg = "Failed to parse SSH Connection configuration file (file path: "
                    + file.getAbsolutePath() + "). " + ((originalMsg == null) ? "" : "Details: " + originalMsg);
            System.out.println(msg);
        }
        if (sshConnectionProperties == null) {
            System.out.println("Failed to retrieve SSH Connection configuration. " +
                    "Make sure that " + file.getAbsolutePath() + " exists and is configured correctly");
        }
        return sshConnectionProperties;
    }
}
