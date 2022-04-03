package com.example.sachin.myDebezium.vo;

import com.google.gson.Gson;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

@NoArgsConstructor
@AllArgsConstructor
@Slf4j
@Builder
@Data
@EqualsAndHashCode
public class Address {
    private int id;
    private String city;
    private int zip;
    private int userId;

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof Address && ((Address)other).getId() == this.getId() ) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
