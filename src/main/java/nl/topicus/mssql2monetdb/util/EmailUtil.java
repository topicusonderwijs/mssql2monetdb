package nl.topicus.mssql2monetdb.util;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import nl.topicus.mssql2monetdb.CONFIG_KEYS;
import nl.topicus.mssql2monetdb.CopyToolConfig;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

public class EmailUtil
{
	private static final Logger LOG = Logger.getLogger(EmailUtil.class);

	/**
	 * Sends an email with a given message en subject. The dataproperties provide the email settings en therefore have to be given to teh function.
	 */
	public static void sendMail(Exception e, Properties databaseProperties)
	{
		EmailUtil.sendMail(e.getMessage() + "\n\n" + ExceptionUtils.getStackTrace(e), databaseProperties);
	}
	
	/**
	 * Sends an email with a given message en subject. The dataproperties provide the email settings en therefore have to be given to teh function.
	 */
	public static void sendMail(String message, Properties databaseProperties)
	{
		sendMail(message, databaseProperties.getProperty(CONFIG_KEYS.MAIL_SUBJECT.toString()), databaseProperties);
	}
	
	/**
	 * Sends an email with a given message en subject. The dataproperties provide the email settings en therefore have to be given to teh function.
	 */
	public static void sendMail(String message, String subject, Properties databaseProperties)
	{
		final String username = databaseProperties.getProperty(CONFIG_KEYS.MAIL_USERNAME.toString()); 
		final String password = databaseProperties.getProperty(CONFIG_KEYS.MAIL_PASSWORD.toString());
		final String from = databaseProperties.getProperty(CONFIG_KEYS.MAIL_FROM.toString());
		final String to = databaseProperties.getProperty(CONFIG_KEYS.MAIL_TO.toString());
		final String server = databaseProperties.getProperty(CONFIG_KEYS.MAIL_SERVER.toString());
		final String port = databaseProperties.getProperty(CONFIG_KEYS.MAIL_PORT.toString());
						
		final boolean isEnabled = CopyToolConfig.getBooleanProperty(databaseProperties, CONFIG_KEYS.MAIL_ENABLED.toString());
		
		if (StringUtils.isEmpty(subject))
			subject = "Error in MSSQL2MonetDB job";
		
		if (!isEnabled)
		{
			LOG.info("Not sending e-mail, error mails disabled");
			return;
		}
		
		if (StringUtils.isEmpty(server) || StringUtils.isEmpty(port))
		{
			LOG.warn("Not sending e-mail, no e-mail properties set");
			return;
		}

		Properties props = new Properties();
		props.put("mail.smtp.host", server);
		props.put("mail.smtp.port", port);
		Session session = null;
		// if username and password is configured, make sure to use authentication
		if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password))
		{
			props.put("mail.smtp.auth", "true");
			props.put("mail.smtp.starttls.enable", "true");
			session = Session.getInstance(props, new javax.mail.Authenticator()
			{
				@Override
				protected PasswordAuthentication getPasswordAuthentication()
				{
					return new PasswordAuthentication(username, password);
				}
			});
		}
		else
		{
			session = Session.getInstance(props);
		}

		try
		{
			LOG.info("Sending e-mail to " + to);
			Message emailMessage = new MimeMessage(session);
			emailMessage.setFrom(new InternetAddress(from));
			emailMessage.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
			emailMessage.setSubject(subject);
			emailMessage.setText(message);

			Transport.send(emailMessage);
			LOG.info("E-mail sent!");
		}
		catch (MessagingException e)
		{
			LOG.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}
	
}
