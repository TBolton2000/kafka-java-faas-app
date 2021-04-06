package tbolton;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent.KafkaEventRecord;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonParseException;
import com.google.gson.JsonDeserializer;
import com.google.gson.reflect.TypeToken;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.lang.reflect.Type;

public class KafkaEventDeserializer implements JsonDeserializer<KafkaEvent> {
  private static final Logger logger = LoggerFactory.getLogger(InvokeTest.class);
  Gson gson = new GsonBuilder().setPrettyPrinting().create();
  Type kafkaEventRecordMap = new TypeToken<Map<String,ArrayList<KafkaEventRecord>>>(){}.getType();

  @Override
  public KafkaEvent deserialize(JsonElement eventJson, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
      KafkaEvent event = new KafkaEvent();
      logger.info("DESERIALIZING TEST EVENT");
      logger.info("EVENT JSON: " + eventJson.toString());
      // Records key is capitalized in test event, but lowercase in type
      String eventSource = eventJson.getAsJsonObject().get("eventSource").getAsString();
      String bootstrapServers = eventJson.getAsJsonObject().get("bootstrapServers").getAsString();
      JsonObject recordsMap = eventJson.getAsJsonObject().get("records").getAsJsonObject();
      Map<String,List<KafkaEventRecord>> records = gson.fromJson(recordsMap, kafkaEventRecordMap);
      event.setRecords(records);
      event.setEventSource(eventSource);
      event.setBootstrapServers(bootstrapServers);
      return event;
  }
}