package com.rcforte;

import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Price {
  private String symbol;
  private Double price;
  private String date;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
