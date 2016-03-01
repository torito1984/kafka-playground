package com.kafka.message.server.example.core;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Created by david on 2/27/16.
 */
public class KafkaMailProducer extends Thread {
	private final KafkaProducer<Integer, String> producer;
	private final String topic;
	private final String directoryPath;
	private final Properties props = new Properties();
    private Random rnd;

	/**
	 * Instantiates a new kafka producer.
	 *
	 * @param topic the topic
	 * @param directoryPath the directory path
	 */
	public KafkaMailProducer(String topic, String directoryPath) {
		this(topic, directoryPath, KafkaMailProperties.kafkaServerPort);
	}

	public KafkaMailProducer(String topic, String directoryPath, int port) {
		props.put("key.serializer", "com.kafka.message.server.example.adapt.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("partitioner.class", "com.kafka.message.server.example.adapt.CustomPartitioner");
		props.put("bootstrap.servers", KafkaMailProperties.kafkaServerURL + ":" + port);
		producer = new KafkaProducer<Integer, String>(props);
		this.topic = topic;
		this.directoryPath = directoryPath;
		rnd = new Random();
	}

	public void run() {
        Path dir = Paths.get(directoryPath);
        try {
        	new WatchDir(dir).start();
			new ReadDir(dir).start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * The Class WatchDir: once directory is clean, it watches for new files added.
	 */
	private class WatchDir extends Thread{
		private WatchKey key; 
		private final Path directory;
		private WatchService watcher;
	    
	 
	    @SuppressWarnings("unchecked")
	    <T> WatchEvent<T> cast(WatchEvent<?> event) {
	        return (WatchEvent<T>)event;
	    }

	    /**
	     * Creates a WatchService and registers the given directory
	     */
	    WatchDir(Path directory)  {
	    	this.directory = directory;
	    	try{
		        this.watcher = FileSystems.getDefault().newWatchService();
		        this.key = directory.register(watcher, ENTRY_CREATE);
	    	} catch (IOException ex) {
            	ex.printStackTrace();
            }
	    }
	 
	    /**
	     * Process all events for keys queued to the watcher
	     */
	    public void run() {
	        for (;;) {
	            for (WatchEvent<?> event: key.pollEvents()) {
	                Kind<?> kind = event.kind();
	 
	                // TBD - provide example of how OVERFLOW event is handled
	                if (kind == OVERFLOW) {
	                    continue;
	                }
	 
	                // Context for directory entry event is the file name of entry
	                WatchEvent<Path> ev = cast(event);
	                Path name = ev.context();
	                Path child = directory.resolve(name);
	 
	                 if (kind == ENTRY_CREATE) {
	                    try {
	                        if (!Files.isDirectory(child, NOFOLLOW_LINKS)) {
	                        	pushFileContent(child.toFile());
	                         }
	                    } catch (IOException ex) {
	                    	ex.printStackTrace();
	                    }
	                }
	            }
	 
	            boolean valid = key.reset();
	            if (!valid) 
	                break;
	        }
	    }
	    
	}
	
	/**
	 * The Class ReadDir: pushes all messages before start watching
	 */
	class ReadDir extends Thread{
		private final Path directory;
		
		ReadDir(Path directory) throws IOException {
	    	this.directory = directory;
	    }
		
		 public void start() {
		        File file = directory.toFile();
      
				List<File> dirMessages = Arrays.asList(file.listFiles(new FileFilter() {
					@Override
					public boolean accept(File pathname) {
						if(pathname.isFile() && !pathname.isHidden()) return true;
						return false;
					}
				}));
				
				for(File temp :dirMessages){
					try {
						pushFileContent(temp);
					} catch (IOException ex) {
						ex.printStackTrace();
					}
				}
		 }

	}
	
  /**
   * Read file content.
   *
   * @param file the file
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private void pushFileContent(File file) throws IOException{
		
	  	RandomAccessFile aFile = new RandomAccessFile(file, "r");
	    FileChannel inChannel = aFile.getChannel();
	    MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
	    buffer.load();  
	    StringBuilder strBuilder = new StringBuilder();
	    for (int i = 0; i < buffer.limit(); i++){
	    	strBuilder.append((char) buffer.get());
	    }
	    buffer.clear(); // do something with the data and clear/compact it.
	    inChannel.close();
	    aFile.close();

	  	int key = new Integer(rnd.nextInt(255));
	  	String value = strBuilder.toString();

	    producer.send(new ProducerRecord<Integer, String>(topic, key, value),
				new DemoCallBack(file.getName()));

	    file.delete();
  }

  private class DemoCallBack implements Callback {

		private final String fileName;

		public DemoCallBack(String fileName) {
			this.fileName = fileName;
		}

		/**
		 * A callback method the user can implement to provide asynchronous handling of request completion.
		 * This method will be called when the record sent to the server has been acknowledged.
		 * Exactly one of the arguments will be non-null.
		 *
		 * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
		 *                  occurred.
		 * @param exception The exception thrown during processing of this record. Null if no error occurred.
		 */
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			if (metadata != null) {
				System.out.println("Producer: " + fileName + " - content consumed.");
			} else {
				exception.printStackTrace();
			}
		}
	}
}
