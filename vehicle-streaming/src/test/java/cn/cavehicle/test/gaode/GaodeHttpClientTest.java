package cn.cavehicle.test.gaode;

import cn.cavehicle.entity.VehicleLocationModel;
import cn.cavehicle.utils.GeoHashUtil;
import cn.cavehicle.utils.RedisUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class GaodeHttpClientTest {

	public static void main(String[] args) throws IOException {
		// 请求地址
		String uri = "https://restapi.amap.com/v3/geocode/regeo?key=5af156b1eff3557d0bc89b8058bff45f&location=116.153459,39.872749" ;

		// 使用Apache HttpClient库，通过HTTP方式请求
		HttpGet httpGet = new HttpGet(uri) ;
		CloseableHttpResponse response = HttpClients.createDefault().execute(httpGet);

		int statusCode = response.getStatusLine().getStatusCode();
		System.out.println("HTTP请求响应状态码：" + statusCode);

		// 获取响应值
		String responseJson = EntityUtils.toString(response.getEntity());
		System.out.println(responseJson);

		// todo: 解析JSON字符串数据，获取字段的值，封装到实体类对象
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
		System.out.println(locationModel);

		// 保存到Redis数据库中, key: 经纬度对应geoHash， value: json字符串
		String geoHash = GeoHashUtil.encode(39.872749, 116.153459);
		String locationJson = JSON.toJSONString(locationModel) ;
		RedisUtil.setValue(geoHash, locationJson);

		System.out.println(RedisUtil.getValue(geoHash));

	}

}
