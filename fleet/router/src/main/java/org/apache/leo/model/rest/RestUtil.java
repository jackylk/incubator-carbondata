package org.apache.leo.model.rest;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

public class RestUtil {

  private static MediaType JSON = MediaType.get("application/json; charset=utf-8");

  public static void postAsync(String url, String json, Callback callback, String token,
      OkHttpClient client) {
    RequestBody body = RequestBody.create(JSON, json);
    Request.Builder builder = new Request.Builder().url(url).post(body);
    if (token != null) {
      builder = builder.addHeader("X-Auth-Token", token);
    }
    final Request request = builder.build();
    Call call = client.newCall(request);
    call.enqueue(callback);
  }

  public static void postAsync(String url, String json, Callback callback, OkHttpClient client) {
    postAsync(url, json, callback, null, client);
  }
}
