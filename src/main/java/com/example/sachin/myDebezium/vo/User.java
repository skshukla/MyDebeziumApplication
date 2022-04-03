package com.example.sachin.myDebezium.vo;

import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

@NoArgsConstructor
@AllArgsConstructor
@Slf4j
@Builder
@Data
public class User {
    private int id;

    private String firstName;

    private String lastName;

    private String email;

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
