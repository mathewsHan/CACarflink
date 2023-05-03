package cn.cavehicle.streaming.function.async;

import cn.cavehicle.entity.VehicleDataPartObj;
import cn.cavehicle.entity.VehicleLocationModel;
import cn.cavehicle.utils.GeoHashUtil;
import cn.cavehicle.utils.RedisUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 *自定义异步函数，实现异步请求Gaode API获取地理位置信息
 */
public class AsyncGaodeHttpQueryFunction extends RichAsyncFunction<VehicleDataPartObj, VehicleDataPartObj> {

	// 定义变量： 异步请求Client对象
	private CloseableHttpAsyncClient httpAsyncClient = null ;
	// 定义变量：Job运行时全局参数
	ParameterTool parameterTool = null ;

	@Override
	public void open(Configuration parameters) throws Exception {
		parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

		// 实例化对象
		RequestConfig config = RequestConfig.custom().setConnectTimeout(30000).setSocketTimeout(10000).build();
		httpAsyncClient = HttpAsyncClients.custom()
			.setMaxConnTotal(5)
			.setDefaultRequestConfig(config)
			.build();
		// 启动执行
		httpAsyncClient.start();
	}

	@Override
	public void asyncInvoke(VehicleDataPartObj input, ResultFuture<VehicleDataPartObj> resultFuture) throws Exception {
		// 1. 获取经纬度
		Double lng = input.getLng();
		Double lat = input.getLat();
		// 2. 拼凑字符串，获取请求访问uri地址
		String uri = parameterTool.getRequired("gaode.url")
			+ "key=" + parameterTool.getRequired("gaode.key")
			+ "&location=" + lng +"," + lat ;
		System.out.println(uri);
		// 3. 异步请求高德API
		HttpGet httpGet = new HttpGet(uri) ;
		Future<HttpResponse> future = httpAsyncClient.execute(httpGet, null);
		// 4. 异步接收返回结果并处理
		CompletableFuture<VehicleDataPartObj> completableFuture = CompletableFuture.supplyAsync(new Supplier<VehicleDataPartObj>() {
			@SneakyThrows
			@Override
			public VehicleDataPartObj get() {
				// 4-1. 获取http请求响应值
				HttpResponse response = future.get();
				// 4-2. 请求响应状态码，当等于200时，表示请求OK，解析JSON字符串
				int statusCode = response.getStatusLine().getStatusCode();
				if(200 == statusCode){
					// 4-3. 获取响应值
					String responseJson = EntityUtils.toString(response.getEntity());
					// 4-4. 解析JSON字符串数据，获取字段的值，封装到实体类对象
					VehicleLocationModel locationModel = new VehicleLocationModel();
					// 第一层解析： 整体字符串解析
					JSONObject jsonObject = JSON.parseObject(responseJson);
					// 第二层解析
					JSONObject object = jsonObject.getJSONObject("regeocode");
					locationModel.setAddress(object.getString("formatted_address"));
					// 第三层解析
					JSONObject addressComponent = object.getJSONObject("addressComponent");
					locationModel.setCountry(addressComponent.getString("country"));
					locationModel.setProvince(addressComponent.getString("province"));
					String city = addressComponent.getString("city");
					if(StringUtils.isEmpty(city) || "[]".equals(city)){
						city = addressComponent.getString("province") ;
					}
					locationModel.setCity(city);
					locationModel.setDistrict(addressComponent.getString("district"));
					locationModel.setTownship(addressComponent.getString("township"));
					locationModel.setLng(lng);
					locationModel.setLat(lat);

					// 5. 保存到Redis数据库中, key: 经纬度对应geoHash， value: json字符串
					String geoHash = GeoHashUtil.encode(lat, lng);
					String locationJson = JSON.toJSONString(locationModel) ;
					RedisUtil.setValue(geoHash, locationJson);

					// 6. 设置车辆数据中位置属性字段
					input.setProvince(locationModel.getProvince());
					input.setCity(locationModel.getCity());
					input.setCounty(locationModel.getDistrict());
					input.setAddress(locationModel.getAddress());
				}else{
					System.out.println("请求高德地图API，响应状态码为：" + statusCode + "........................");
				}
 				return input;
			}
		});
		// 5. 将异步处理结果数据返回
		completableFuture.thenAccept(new Consumer<VehicleDataPartObj>() {
			@Override
			public void accept(VehicleDataPartObj vehicleDataPartObj) {
				resultFuture.complete(Collections.singletonList(vehicleDataPartObj));
			}
		});
	}

	// 异步请求超时如何处理
	@Override
	public void timeout(VehicleDataPartObj input, ResultFuture<VehicleDataPartObj> resultFuture) throws Exception {
		System.out.println("请求高德地图API接口超时啦.............................");
	}

	@Override
	public void close() throws Exception {
		if(null != httpAsyncClient && !httpAsyncClient.isRunning()){
			httpAsyncClient.close();
		}
	}
}
