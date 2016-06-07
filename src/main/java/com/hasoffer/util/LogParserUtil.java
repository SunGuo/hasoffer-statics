package com.hasoffer.util;


import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.hadoop.io.Text;
import org.hsqldb.lib.StringUtil;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;



/**
 * Created by guoxian1 on 16/6/1.
 */
public class LogParserUtil {

    public static Map<String, String> getHasofferPingback(String line){
        return null;
    }


    public static Map<String, String> getPingbackMap(String line) {
        int index = line.indexOf("/hm.png?");
        Map<String, String> para = new HashMap<String, String>();

        if (index > 0) {
            String LogKV = line.substring(index + 8);
            int index1 = LogKV.indexOf(" ");
            if(index1 > 0){
                LogKV = LogKV.substring(0, index1);
                String[] KVs = LogKV.split("&");
                for (String t : KVs) {
                    try {
                        String[] value = t.split("=");
                        if(value.length >=2){
                            para.put(value[0].trim(), value[1].trim());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return para;
    }

    public static Map<String, String> getAbroadPingback(String line) {
        int index = line.indexOf("/upload_mes?");
        Map<String, String> para = new HashMap<String, String>();

        if (index > 0) {
            String LogKV = line.substring(index + 12);
            int index1 = LogKV.indexOf(" ");
            if(index1 > 0){
                LogKV = LogKV.substring(0, index1);

                String[] KVs = LogKV.split("&");
                for (String t : KVs) {
                    try {
                        String[] value = t.split("=");
                        if(value.length >=2){
                            para.put(value[0].trim(),  URLDecoder.decode(value[1].trim()));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return para;
    }


    public static Map<String, Object> getHasofferLog(String line) {
        int index = line.replace("\\x22", "\"").indexOf(" \"{");
        Map<String, Object> para = new HashMap<String, Object>();

        if (index > 0) {
            String LogKV = line.substring(index + 2);
            int index1 = LogKV.indexOf("}\"");
            if(index1 > 0){
                LogKV = LogKV.substring(0, index1 + 1);
                JSONObject jsonObject = JSONObject.fromObject(LogKV);
                for(Iterator iter = jsonObject.keys(); iter.hasNext();){
                    String key = (String)iter.next();
                    para.put(key, jsonObject.get(key));
                }

            }
        }
        return para;
    }

    public static void main(String [] args){
        String tmp = "101.221.133.2 - - [29/May/2016:03:12:04 +0800] \"GET /app/config HTTP/1.1\" \"{\\x22deviceId\\x22:\\x228d435235bad35365\\x22,\\x22imeiId\\x22:\\x22\\x22,\\x22deviceName\\x22:\\x22asus ASUS_Z010D WW_Phone\\x22,\\x22brand\\x22:\\x22asus\\x22,\\x22osVersion\\x22:\\x225.0.2\\x22,\\x22serial\\x22:\\x22FCAXB7011904HZF\\x22,\\x22appVersion\\x22:\\x22224\\x22,\\x22screen\\x22:\\x221280x720\\x22,\\x22shopApp\\x22:[\\x22FLIPKART\\x22,\\x22SNAPDEAL\\x22],\\x22otherApp\\x22:[\\x22CLEANMASTER\\x22],\\x22curShopApp\\x22:\\x22SNAPDEAL\\x22,\\x22screenSize\\x22:\\x225.464508726613722\\x22,\\x22appCount\\x22:208,\\x22ramSize\\x22:\\x221984397312\\x22,\\x22curNetState\\x22:\\x22wifi\\x22,\\x22appType\\x22:\\x22SDK\\x22,\\x22marketChannel\\x22:\\x22LeoMaster\\x22,\\x22mac\\x22:\\x229c:5c:8e:7d:59:87\\x22}\" 404 168 \"-\" \"Dalvik/2.1.0 (Linux; U; Android 5.0.2; ASUS_Z010D Build/LRX22G)\" \"-\"";
        Map<String, Object> para = LogParserUtil.getHasofferLog(tmp.replace("\\x22", "\"").replace("\\x5C", "\\"));
        System.out.println(CommonUtils.getDeviceId(para));

        String deviceId = CommonUtils.getDeviceId(para);
        JSONArray shopApp = (JSONArray) para.get("shopApp");
        String channel = null;

        if(para.containsKey("marketChannel")){
            channel = (String) para.get("marketChannel");
        }
        System.out.println(shopApp);
        if(StringUtil.isEmpty(deviceId)){

            if(shopApp != null && shopApp.size() > 0) {
                if (channel != null) {
                    System.out.println(channel);
                } else {
                    System.out.println("all");
                }
            }
        }

        String tt = "106.208.40.69 - - [30/May/2016:03:47:13 +0800] \"GET " +
                "/cmp/getcmpskus?q=Headly+Premium+CP-HR-16KGCOMBO11+Coloured+Gym+%27+Fitness+Kit&price=Rs.7920&site=FLIPKART HTTP/1.1\" \"{\\x22deviceId\\x22:\\x224e28c7da041d0911\\x22,\\x22imeiId\\x22:\\x22\\x22,\\x22deviceName\\x22:\\x22asus ASUS_Z010D WW_Phone\\x22,\\x22brand\\x22:\\x22asus\\x22,\\x22osVersion\\x22:\\x225.0.2\\x22,\\x22serial\\x22:\\x22G1AXB702D023ES2\\x22,\\x22appVersion\\x22:\\x22222\\x22,\\x22screen\\x22:\\x221280x720\\x22,\\x22shopApp\\x22:[\\x22FLIPKART\\x22],\\x22otherApp\\x22:[\\x22CLEANMASTER\\x22],\\x22curShopApp\\x22:\\x22FLIPKART\\x22,\\x22appType\\x22:\\x22SDK\\x22,\\x22marketChannel\\x22:\\x22SHANCHUAN\\x22,\\x22mac\\x22:\\x22d0:17:c2:3e:73:09\\x22}\" 200 624 \"-\" \"Dalvik/2.1.0 (Linux; U; Android 5.0.2; ASUS_Z010D Build/LRX22G)\" \"-\"";
        String [] values  = tt.split(" ");
        System.out.println(values[6]);
//        String deviceId = (String) para.get("deviceId");
//        String channel = (String) para.get("marketChannel");
//        System.out.println(para.get("asdfsf"));
//        JSONArray shopApp = (JSONArray) para.get("shopApp");
//        System.out.println(shopApp.size());
//        System.out.println(deviceId + "  " + channel);
//        if()
//        System.out.println((tmps[8].replace("\\x22", "\"")));
    }


}
