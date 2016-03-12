package com.kafka.message.server.example.launch;

import java.util.ArrayList;
import java.util.List;

import com.kafka.message.server.example.util.DefaultProperties;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import com.kafka.message.server.example.core.KafkaMailProducer;
import com.kafka.message.server.example.core.KafkaMailProperties;
import com.kafka.message.server.example.util.CommandLineHandler;
import com.kafka.message.server.example.util.FileUtil;
import com.kafka.message.server.example.util.PropertyKeys;
import static com.kafka.message.server.example.util.GlobalNames.*;

/**
 * The Class MailProducerDemo.
 * 
 * @author Abhishek Sharma, david martinez
 */
public class MailProducerDemo {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		try {
			CommandLineHandler commandLine  = new CommandLineHandler(getProducerOptions(), args);
			
			String topic = commandLine.getOption(TOPIC_OPTION_NAME);
			String path  = commandLine.getOption(PATH);
			
			// start producer thread
			KafkaMailProducer producerThread = new KafkaMailProducer(topic!=null? topic : KafkaMailProperties.topic, 
					path !=null? FileUtil.getValidDirectoryPath(path) :	DefaultProperties.getPropertyValue(PropertyKeys.MAIL_DIRECTORY));
			producerThread.start();
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Gets the producer options.
	 *
	 * @return the producer options
	 */
	private static List<Option> getProducerOptions(){
		List<Option> optionList = new ArrayList<Option>();
		
		Option topicOption = new Option(TOPIC_OPTION_NAME, TOPIC_OPTION_NAME, true, "topic name on which message is going to be published");
		Option pathOption = new Option(PATH, PATH, true, "directory path from where message content going to be consumed.");

		optionList.add(topicOption);
		optionList.add(pathOption);
		
		return optionList;
	}

}
