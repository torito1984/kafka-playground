package com.kafka.message.server.example.launch;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import com.kafka.message.server.example.core.KafkaMailConsumer;
import com.kafka.message.server.example.core.KafkaMailProperties;
import com.kafka.message.server.example.util.CommandLineHandler;

import static com.kafka.message.server.example.util.GlobalNames.*;

/**
 * Created by david on 2/27/16.
 */
public class MailConsumerDemo {

	/**
	 * Gets the producer options.
	 *
	 * @return the producer options
	 */
	private static List<Option> getProducerOptions(){
		List<Option> optionList = new ArrayList<Option>();
		
		Option topicOption = new Option(TOPIC_OPTION_NAME, TOPIC_OPTION_NAME, true, "topic name on which message is going to be published");
		optionList.add(topicOption);
		Option offsetOption = new Option(OFFSET_OPTION_NAME, OFFSET_OPTION_NAME, true, "offset for the consumer to start from");
		optionList.add(offsetOption);
		Option groupOption = new Option(GROUP_OPTION_NAME, GROUP_OPTION_NAME, true, "group the consumer appertains to");
		optionList.add(groupOption);

		return optionList;
	}

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		try {
			CommandLineHandler commandLine  = new CommandLineHandler(getProducerOptions(), args);

			String topic = commandLine.getOption(TOPIC_OPTION_NAME);
			String offset = commandLine.getOption(OFFSET_OPTION_NAME);
			String group = commandLine.getOption(GROUP_OPTION_NAME);

			//start consumer thread
			new KafkaMailConsumer(topic!=null && !topic.isEmpty() ? topic : KafkaMailProperties.topic,
					offset!=null&& !offset.isEmpty() ? offset : "largest",
					group!= null && !group.isEmpty() ? group : KafkaMailProperties.groupId).start();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
}
