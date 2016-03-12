package com.kafka.message.server.example.launch;


import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import com.kafka.message.server.example.core.KafkaMailConsumer;
import com.kafka.message.server.example.core.KafkaMailProducer;
import com.kafka.message.server.example.core.KafkaMailProperties;
import com.kafka.message.server.example.util.CommandLineHandler;
import com.kafka.message.server.example.util.FileUtil;
import com.kafka.message.server.example.util.DefaultProperties;
import com.kafka.message.server.example.util.PropertyKeys;

import static com.kafka.message.server.example.util.GlobalNames.*;


/**
 * The Class MailConsumerProducerDemo.
 * 
 * @author Abhishek Sharma
 */
public class MailConsumerProducerDemo {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		try {
			CommandLineHandler commandLine  = new CommandLineHandler(getProducerOptions(), args);
			
			String topic = commandLine.getOption(TOPIC_NAME);
			String path  = commandLine.getOption(PATH);
			String offset = commandLine.getOption(OFFSET_OPTION_NAME);
			String group = commandLine.getOption(GROUP_OPTION_NAME);

			// start producer thread
			new KafkaMailProducer(topic!=null && !topic.isEmpty() ? topic : KafkaMailProperties.topic,
					path !=null && !path.isEmpty() ? FileUtil.getValidDirectoryPath(path) :
							DefaultProperties.getPropertyValue(PropertyKeys.MAIL_DIRECTORY)).start();

			//start consumer thread
			new KafkaMailConsumer(topic!=null && !topic.isEmpty() ? topic : KafkaMailProperties.topic,
					offset!=null&& !offset.isEmpty() ? offset : "largest",
					group!= null && !group.isEmpty() ? group : KafkaMailProperties.groupId).start();
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
		
		Option topicOption = new Option(TOPIC_NAME, TOPIC_NAME, true, "topic name on which message is going to be published");
		optionList.add(topicOption);
		Option pathOption = new Option(PATH, PATH, true, "directory path from where message content going to be consumed.");
		optionList.add(pathOption);
		Option offsetOption = new Option(OFFSET_OPTION_NAME, OFFSET_OPTION_NAME, true, "offset for the consumer to start from");
		optionList.add(offsetOption);

		Option groupOption = new Option(GROUP_OPTION_NAME, GROUP_OPTION_NAME, true, "offset for the consumer to start from");
		optionList.add(groupOption);

		return optionList;
	}

}
