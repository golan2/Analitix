package com.analitix.utils;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.File;

public class SSHConnection {

    public static Session establish(SSHConnectionProperties sshProperties) throws JSchException {
        if (sshProperties.getKeyFilePath() != null) {
            return establishWithKey(
                    sshProperties.getHost(),
                    sshProperties.getPort(),
                    sshProperties.getUser(),
                    sshProperties.getKeyFilePath()
            );
        } else {
            return establishWithPassword(
                    sshProperties.getHost(),
                    sshProperties.getPort(),
                    sshProperties.getUser(),
                    sshProperties.getPassword()
            );
        }
    }

    public static Session establishWithPassword(String sshHost, int sshPort, String user, String password) throws JSchException {
        Session session;
        JSch jsch = new JSch();
        try {
            session = jsch.getSession(user, sshHost, sshPort);
            session.setPassword(password);
        }
        catch (JSchException e) {
            System.out.println("SSH connection attempt to host: " + sshHost + ":" + sshPort + " failed");
            throw e;
        }
        return connect(session, sshHost, sshPort);
    }

    public static Session establishWithKey(String sshHost, int sshPort, String user, String keyFilePath) throws JSchException {
        File keyFile = new File(keyFilePath);
        if (!keyFile.exists()) {
            String errorMsg = "Could not find SSH public key file in path: " + keyFilePath;
            System.out.println(errorMsg);
            throw new JSchException(errorMsg);
        }
        Session session;
        JSch jsch = new JSch();
        try {
            jsch.addIdentity(keyFile.getAbsolutePath());
            session = jsch.getSession(user, sshHost, sshPort);
        }
        catch (JSchException e) {
            System.out.println("SSH connection attempt to host: " + sshHost + ":" + sshPort + " failed");
            throw e;
        }
        return connect(session, sshHost, sshPort);
    }

    private static Session connect(Session session, String sshHost, int sshPort) throws JSchException {
        session.setConfig("StrictHostKeyChecking", "no");
        session.setConfig("ConnectionAttempts", "3");
        System.out.println("Establishing SSH Connection to host: " + sshHost + ":" + sshPort + "...");
        try {
            session.connect();
        }
        catch (JSchException e) {
            System.out.println("SSH connection attempt to host: " + sshHost + ":" + sshPort + " failed");
            throw e;
        }
        System.out.println("Connected to: " + sshHost + ":" + sshPort + " via SSH");
        return session;
    }

}

