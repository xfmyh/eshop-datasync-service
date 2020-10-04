package com.roncoo.eshop.datasync.rabbitmq;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.roncoo.eshop.datasync.service.EshopProductService;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * 数据同步服务，获取各种原子数据的变更消息
 * 1、然后通过spring cloud fegion 调用eshop-product-service服务的各种接口，获取数据
 * 2、将原子数据在redis中进行增删改
 * 3、将唯独数据变化消息写入rabbitmq中的另一个queue ，供数据聚合服务消费
 */
@Component
@RabbitListener(queues = "data-change-queue")
public class DataChangeQueueReceiver {

    @Autowired
    private EshopProductService eshopProductService;

    @Autowired
    private JedisPool jedisPool;

    @Autowired
    private RabbitMQSender rabbitMQSender;

    private Set<String> dimDataChangeMessageSet = Collections.synchronizedSet(new HashSet<>());

    private List<JSONObject> brandDataChangeMessageList = new ArrayList<>();

    public DataChangeQueueReceiver(){
        new SendThread().start();
    }


    @RabbitHandler
    public void process(String message){
        //对这个message进行解析
        System.out.println("从data-change-queue队列接受一条消息：" + message);
        JSONObject jsonObject = JSONObject.parseObject(message);

        String dataType = jsonObject.getString("data_type");

        if("brand".equals(dataType)){
            processBrandDataChangeMessage(jsonObject);
        }else if("category".equals(dataType)){
            processCategoryDataChangeMessage(jsonObject);
        }else if("product".equals(dataType)){
            processProductDataChangeMessage(jsonObject);
        }else if("product_intro".equals(dataType)){
            processProductIntroDataChangeMessage(jsonObject);
        }else if("product_property".equals(dataType)){
            processProductPropertyDataChangeMessage(jsonObject);
        }else if("product_specification".equals(dataType)){
            processProductSpecificationDataChangeMessage(jsonObject);
        }

    }


    private void processBrandDataChangeMessage(JSONObject jsonObject){
        Long id = jsonObject.getLong("id");

        String eventype = jsonObject.getString("event_type");

        if("add".equals(eventype) || "update".equals(eventype)){
            brandDataChangeMessageList.add(jsonObject);
            System.out.println("将品牌数据放入内存list中，list.size=" + brandDataChangeMessageList.size());

            if(brandDataChangeMessageList.size() >= 2){
                System.out.println("将品牌数据内存list大小等于2，开始执行批量调用");

                String ids = "";
                for(int i = 0 ; i < brandDataChangeMessageList.size(); i++){
                    ids += brandDataChangeMessageList.get(i).getLong("id");
                    if(i < brandDataChangeMessageList.size() - 1){
                        ids += ",";
                    }
                }
                System.out.println("品牌数据ids="+ids);

                String brandsJSON =  eshopProductService.findBrandByIds(ids);

                System.out.println("批量品牌数据="+brandsJSON);

                JSONArray brandJSONArray = JSONArray.parseArray(brandsJSON);

                Jedis jedis = jedisPool.getResource();

                for(int i = 0 ; i < brandJSONArray.size() ; i++){
                    JSONObject brandJSONObject = brandJSONArray.getJSONObject(i);
                    jedis.set("brand:" + brandJSONObject.getLong("id")+":",brandJSONObject.toJSONString());
                    System.out.println("将品牌数据写入redis,id=" + brandJSONObject.getLong("id"));

                    dimDataChangeMessageSet.add("{\"dim_type\":\"brand\",\"id\":\"" + brandJSONObject.getLong("id") + "\"}");
                    System.out.println("将品牌数据写入内存去重set中,id=" + brandJSONObject.getLong("id"));

                }
                brandDataChangeMessageList.clear();

            }



        }else if("delete".equals(eventype)){
            Jedis jedis = jedisPool.getResource();

            jedis.del("brand:" + id+":");
            dimDataChangeMessageSet.add("{\"dim_type\":\"brand\",\"id\":\"" + id + "\"}");


        }
        //rabbitMQSender.send("aggr-data-change-queue","{\"dim_type\":\"brand\",\"id\":\"" + id + "\"}");


    }





    private void processCategoryDataChangeMessage(JSONObject jsonObject){
        Long id = jsonObject.getLong("id");

        String eventype = jsonObject.getString("event_type");

        if("add".equals(eventype) || "update".equals(eventype)){
            String categoryJSON =  eshopProductService.findCategoryById(id);
            JSONObject categoryJSONObject = JSONObject.parseObject(categoryJSON);
            Jedis jedis = jedisPool.getResource();

            jedis.set("category:" + categoryJSONObject.getLong("id")+":",categoryJSON);
        }else if("delete".equals(eventype)){
            Jedis jedis = jedisPool.getResource();

            jedis.del("category:" + id+":");

        }
        dimDataChangeMessageSet.add("{\"dim_type\":\"category\",\"id\":\"" + id + "\"}");
        //rabbitMQSender.send("aggr-data-change-queue","{\"dim_type\":\"category\",\"id\":\"" + id + "\"}");

    }




    private void processProductDataChangeMessage(JSONObject jsonObject){
        Long id = jsonObject.getLong("id");

        String eventype = jsonObject.getString("event_type");

        if("add".equals(eventype) || "update".equals(eventype)){
            String productJSON =  eshopProductService.findProductById(id);
            JSONObject productJSONObject = JSONObject.parseObject(productJSON);
            Jedis jedis = jedisPool.getResource();

            jedis.set("product:" + productJSONObject.getLong("id")+":",productJSON);
        }else if("delete".equals(eventype)){
            Jedis jedis = jedisPool.getResource();

            jedis.del("product:" + id+":");

        }
        dimDataChangeMessageSet.add("{\"dim_type\":\"product\",\"id\":\"" + id + "\"}");
        //rabbitMQSender.send("aggr-data-change-queue","{\"dim_type\":\"product\",\"id\":\"" + id + "\"}");

    }



    private void processProductIntroDataChangeMessage(JSONObject jsonObject){
        Long id = jsonObject.getLong("id");
        Long productId = jsonObject.getLong("product_id");

        String eventype = jsonObject.getString("event_type");

        if("add".equals(eventype) || "update".equals(eventype)){
            String productIntroJSON =  eshopProductService.findProductIntroById(id);
            JSONObject productIntroJSONObject = JSONObject.parseObject(productIntroJSON);
            Jedis jedis = jedisPool.getResource();

            jedis.set("product_intro:" + productId+":",productIntroJSON);
        }else if("delete".equals(eventype)){
            Jedis jedis = jedisPool.getResource();

            jedis.del("product_intro:" + productId+":");

        }

        dimDataChangeMessageSet.add("{\"dim_type\":\"product\",\"id\":\"" + productId + "\"}");
        //rabbitMQSender.send("aggr-data-change-queue","{\"dim_type\":\"product\",\"id\":\"" + productId + "\"}");

    }




    private void processProductPropertyDataChangeMessage(JSONObject jsonObject){
        Long id = jsonObject.getLong("id");
        Long productId = jsonObject.getLong("product_id");

        String eventype = jsonObject.getString("event_type");

        if("add".equals(eventype) || "update".equals(eventype)){
            String productPropertyJSON =  eshopProductService.findProductPropertyById(id);
            JSONObject productPropertyJSONObject = JSONObject.parseObject(productPropertyJSON);
            Jedis jedis = jedisPool.getResource();

            jedis.set("product_property:" + productId+":",productPropertyJSON);
        }else if("delete".equals(eventype)){
            Jedis jedis = jedisPool.getResource();

            jedis.del("product_property:" + productId+":");

        }
        dimDataChangeMessageSet.add("{\"dim_type\":\"product\",\"id\":\"" + productId + "\"}");
        //rabbitMQSender.send("aggr-data-change-queue","{\"dim_type\":\"product\",\"id\":\"" + productId + "\"}");

    }

    private void processProductSpecificationDataChangeMessage(JSONObject jsonObject){
        Long id = jsonObject.getLong("id");
        Long productId = jsonObject.getLong("product_id");

        String eventype = jsonObject.getString("event_type");

        if("add".equals(eventype) || "update".equals(eventype)){
            String productSpecificationJSON =  eshopProductService.findProductSpecificationById(id);
            JSONObject productSpecificationJSONObject = JSONObject.parseObject(productSpecificationJSON);
            Jedis jedis = jedisPool.getResource();

            jedis.set("product_specification:" + productId+":",productSpecificationJSON);
        }else if("delete".equals(eventype)){
            Jedis jedis = jedisPool.getResource();

            jedis.del("product_specification:" + productId+":");

        }

        dimDataChangeMessageSet.add("{\"dim_type\":\"product\",\"id\":\"" + productId + "\"}");
        //rabbitMQSender.send("aggr-data-change-queue","{\"dim_type\":\"product\",\"id\":\"" + productId + "\"}");

    }


    private class SendThread extends  Thread{

        @Override
        public void run() {
            while (true){
                try {
                    if(!dimDataChangeMessageSet.isEmpty()){
                        for(String message : dimDataChangeMessageSet){
                            try {
                                rabbitMQSender.send("aggr-data-change-queue",message);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                        dimDataChangeMessageSet.clear();
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                    }
                }catch (Exception e){
                    e.printStackTrace();
                }

            }
        }
    }

}
