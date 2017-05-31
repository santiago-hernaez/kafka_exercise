package com.kschool;

import com.kschool.kafka.Producers;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Processor extends Thread {
    LinkedBlockingQueue<Map<String, Object>> inQueue;
    LinkedBlockingQueue<Map<String, Object>> outQueue;

    LocalMemCache localMemCache = new LocalMemCache();

    public enum Alert {
        USER_ACCESS_WITHOUT_EXIT(0), USER_EXIT_WITHOUT_ACCESS(1);

        public Integer alert;

        Alert(Integer alert) {
            this.alert = alert;
        }
    }

    public enum Control {
        OPEN(0), NOT_OPEN(1);

        public Integer action;

        Control(Integer action) {
            this.action = action;
        }
    }

    public enum EventType {
        CONTROL("control"), ALERT("alert"), METRIC("metric");

        public String type;

        EventType(String name) {
            this.type = name;
        }
    }

    public Processor(LinkedBlockingQueue<Map<String, Object>> inQueue,
                         LinkedBlockingQueue<Map<String, Object>> outQueue) {
        this.inQueue = inQueue;
        this.outQueue = outQueue;
    }
    public String convertTime(long time){
        //Date date = new Date(time);
        //Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long second = (time /1000)%60;
        long minutes = (time/(1000*60))%60;
        long hours = (time/(1000*60*60))%24;
        return String.format("%02d:%02d:%02d",hours,minutes,second);
        //return format.format(date);
    }

    public void callProducers(String type, Map<String,Object> inEvent, Long diff){
        if (type == "alert") {
            inEvent.put("type", "alert");
            try {
                outQueue.put(inEvent);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (diff == 0L) {
                Producers alerta = new Producers("alerta", "NOT_OPEN", "0", "USER_ACCESS_WITHOUT_EXIT", outQueue);
            } else {
                Producers alerta = new Producers("alerta","NOT_OPEN","0","USER_EXIT_WITHOUT_ACCESS",outQueue);
            }
        }
        else if (type == "control"){
            inEvent.put("type","control");
            try {
                outQueue.put(inEvent);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (diff==0L) {
                Producers control = new Producers("control", "NOT_OPEN", "0", "USER_ACCESS_WITHOUT_EXIT", outQueue);
            } else {
                Producers control = new Producers("control","NOT_OPEN","0","USER_EXIT_WITHOUT_ACCESS",outQueue);
            }
        }
        else if (type=="metric"){
            inEvent.put("type","metric");
            inEvent.remove("action");
            try {
                outQueue.put(inEvent);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Producers metrica = new Producers("metrica","OPEN",convertTime(diff),"0",outQueue);
            localMemCache.removeUser((String)inEvent.get("user_id"));
        }

    }

    @Override
    public void run() {
        while (!isInterrupted()) {
            try {
                Map<String, Object> inEvent = inQueue.take();

                // "timestamp":1442938374, "user_id":"ADB123456","action":0-1,"full-name":"Andres Gomez"
                // action: 0 al entrar, 1 al salir.

                int accion = (int) inEvent.get("action");
                //si el usuario quiere entrar
               if (accion == 0) {
                   //comprueba si el usuario esta en el sistema
                   if (localMemCache.userExist((String) inEvent.get("user_id"))){
                       callProducers("alert", inEvent,0L);
                       System.out.println("ALERTA! entrando sin salir. La puerta permanece CERRADA");
                       callProducers("control",inEvent,0L);
                   }
                   //Si el usuario no está en el sistema:
                  else {
                       callProducers("control",inEvent,0L);
                       Timestamp entrada = new Timestamp(((long)(inEvent.get("timestamp")))+3600L);
                       localMemCache.saveUserTime((String)inEvent.get("user_id"),entrada);
                       System.out.println("ABRIMOS la puerta de entrada");
                   }
               }
               //si el usuario quiere salir
               else {
                   //si el usuario esta en el sistema
                   if (localMemCache.userExist((String) inEvent.get("user_id"))){
                       callProducers("control",inEvent,0L);
                       Timestamp salida = new Timestamp( ((long)(inEvent.get("timestamp")))+3600L);
                       Timestamp entrada = new Timestamp(System.currentTimeMillis());
                       entrada = localMemCache.getUserTime((String)(inEvent.get("user_id")));
                       long diff = salida.getTime() - entrada.getTime();
                       inEvent.put("duration",convertTime(diff));
                       callProducers("metric",inEvent,diff);
                       System.out.println("ABRIMOS la puerta de salida.");

                   }
                   //Si el usuario no esta en el sistema.
                   else {
                       callProducers("alert",inEvent,1L);
                       System.out.println("ALERTA! saliendo sin entrar. La puerta permanece CERRADA");
                       callProducers("control",inEvent,1L);
                   }
               }



            } catch (InterruptedException e) {
                System.out.println("Apagando el procesador ... ");

            }
        }
    }



    public void shutdown() {
        interrupt();
    }

    private class LocalMemCache {
        private Map<String, Timestamp> cache = new HashMap<>();

        public void saveUserTime(String userId, Timestamp timestamp) {
            cache.put(userId, timestamp);
        }

        public Boolean userExist(String userId) {
            return cache.containsKey(userId);
        }

        public Timestamp getUserTime(String userId) {
            return cache.get(userId);
        }

        public void removeUser(String userId) {
            cache.remove(userId);
        }
    }
}
