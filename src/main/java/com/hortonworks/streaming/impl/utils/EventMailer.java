package com.hortonworks.streaming.impl.utils;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class EventMailer {
	private static Properties props;

	static {
		props = new Properties();
		props.put("mail.smtp.host", "sandbox");
		props.put("mail.smtp.port", "25");
	}

	public static void sendEmail(String sender, String recipient,
			String subject, String body) {

		Session session = Session.getInstance(props);

		try {
			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(sender));
			message.setRecipients(Message.RecipientType.TO,
					InternetAddress.parse(recipient));
			
			message.setSubject(subject);
			message.setText(body);
			
			Transport.send(message);
		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
	}
}
