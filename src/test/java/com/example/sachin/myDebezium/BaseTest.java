package com.example.sachin.myDebezium;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@Slf4j
class BaseTest {

		protected Gson GSON = new GsonBuilder()
			.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
			.create();

	@Test
	void contextLoads() {
	}

	@Test
	public void me() {
		String s = "{\"before\":null,\"after\":{\"id\":1,\"first_name\":\"Sachin-1\",\"last_name\":\"Shukla-1\",\"email\":\"e1@gmail.com\"}}";


		log.info(s);
	}

}
