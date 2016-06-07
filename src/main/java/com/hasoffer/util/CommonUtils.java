package com.hasoffer.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;

/**
 * Created by guoxian1 on 16/6/3.
 */
public class CommonUtils {

    private static String deviceContent = "id,createTime,updateTime,brand,mac,deviceId,imeiId,serial,deviceName,osVersion,screen,appVersion,shopApp,otherApp,appType,marketChannel,screenSize,ramSize,appCount,gcmToken";

    public static String getDeviceId(Map<String, Object> para){
        StringBuffer deviceIdBuf = new StringBuffer();

    /*if (StringUtils.isEmpty(di.getDeviceId()) && StringUtils.isEmpty(di.getImeiId())) {
        logger.debug("device id is null ! imei id is null ! ");
        return;
    }*/

        String clientDeviceId = (String) para.get("deviceId");
        String imeiId = (String) para.get("imeiId");
        String serialNo = (String) para.get("serial");

        if (!StringUtils.isEmpty(clientDeviceId)) {
            deviceIdBuf.append(clientDeviceId);
        }

        if (!StringUtils.isEmpty(imeiId)) {
            deviceIdBuf.append(imeiId);
        }

        String deviceIdBufStr = deviceIdBuf.toString();
        if (StringUtils.isEmpty(deviceIdBufStr)) {
            deviceIdBufStr = serialNo;
            //logger.debug(String.format("device id is null ! imei id is null ! Get serial : %s.", deviceIdBufStr));
        }

        return new String(Hex.encodeHex(DigestUtils.md5(deviceIdBufStr)));
    }


    public static Map<String, String> ParseDeviceTable(String line){

        String [] deviceContents = CommonUtils.deviceContent.split(",");

        Map<String, String> para = new HashMap<String, String>();


        String [] contents = line.split(",");

        int i = 0;

        for(String t: contents){
            para.put(deviceContents[i], t);
            i++;
        }

        return para;
    }


    public static void main(String [] args){
        String tmp = "063521e0bf9820659128e329cd6e5bd5,2016/02/23 19:37:43,,Spice,,f2b445f44f16508,911451754005026,0123456789ABCDEF," +
                "Spice Xlife-M5+ Spice Xlife-M5+,4.4.2,854x480,1,ASKMEBAZAAR_PAYTM_FLIPKART,,PLUGIN_UI,PREASSEMBLE,,,0,";

        Map<String, String> para = CommonUtils.ParseDeviceTable(tmp);
        System.out.println(para);
    }
}
