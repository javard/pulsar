package org.apache.pulsar.functions.api.examples;

import com.google.gson.JsonArray;
///+import com.sun.tools.javac.util.ArrayUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class DeltasFunction implements Function<String, byte[]>{


    /**
     * Realiza o calculo dos deltas das entradas dois snapshots consecutivos.
     * Para cada entrada verifica se já existia no snpashot anterior e calcula o delta em relação.
     * Caso seja a primeira vez que a entrada é vista, nunhum calculo é efetuado e a entrada e enviada com os valores originais da tabela.
     *
     *
     * stored snapshot format:
     *
     * {
     *     "tableentrykey":<JSONObject with the values of the columns>
     *     "tableentrykey":<JSONObject with the values of the columns>
     *      ...
     *     "tableentrykey":<JSONObject with the values of the columns>
     * }
     *
     * */

    @Override
    public byte[] process(String input, Context context) throws Exception {
        //Long start = System.currentTimeMillis();

        log.info("NEW DELTA:");
        JSONObject newEntry = new JSONObject(input);

        String topic = context.getInputTopics().toArray(new String[0])[0];
        String current_snap_id = newEntry.getJSONObject(newEntry.keys().next()).getString("mon_snapshot_id");
        String instance_name = newEntry.getJSONObject(newEntry.keys().next()).getString("mon_instance");
        String prevSnapshotKey = "prev_snapshot_"+instance_name+"_"+topic;

        log.info("==> "+prevSnapshotKey);
        JSONObject prevSnapshot = getPrevSnapshot(context, prevSnapshotKey);

        if(prevSnapshot != null) {
            log.info("Processing stored snapshot");
            JSONObject activeKeys = getActiveKeys(context,prevSnapshotKey);
            String prev_snap_id = null;
            if(activeKeys != null){
                prev_snap_id = activeKeys.getString("mon_snapshot_id");
            }else{
                resetActiveKeys(context,prevSnapshotKey);
            }

            if (prev_snap_id != null && !prev_snap_id.equals("") && !prev_snap_id.equals(current_snap_id)){
                prevSnapshot = removeInactiveKeys(context, prevSnapshot, prevSnapshotKey);
                if (prevSnapshot.isEmpty()){
                    log.info("SOMETHING WENT WRONG!!");
                }
            }
            log.info("PrevSnapshot ready");
        }else {
            log.info("There is no stored snapshot");
            prevSnapshot = new JSONObject();
            resetActiveKeys(context,prevSnapshotKey);
        }

        JSONObject newStoredSnap = prevSnapshot;
        JSONArray toSend = new JSONArray();
        for(String key: newEntry.keySet()){ //neste caso so ha uma mas podem ser várias ( batch)
            //System.out.println("stored keys: "+prevRecord.keySet());
            //System.out.println(key);
            addToActiveKeys(context, prevSnapshotKey,key,current_snap_id);

            JSONObject toSendEntry = new JSONObject(newEntry.getJSONObject(key).toString());
            if (prevSnapshot.has(key)){
                System.out.println("Key was in store");
                JSONObject updatedRecord =  new JSONObject(newEntry.getJSONObject(key).toString());
                JSONObject deltas = calcDeltas(newEntry.getJSONObject(key).getJSONObject("counters"), prevSnapshot.getJSONObject(key).getJSONObject("counters"));

                updatedRecord.remove("counters");
                updatedRecord.put("counters", deltas);

                long updated_delta = updatedRecord.getLong("mon_delta") - prevSnapshot.getJSONObject(key).getLong("mon_delta");
                System.out.println("Curr delta: "+updatedRecord.getLong("mon_delta")+ " Prev delta: "+ prevSnapshot.getJSONObject(key).getLong("mon_delta"));
                System.out.println("MON_DELTA: "+updated_delta);
                updatedRecord.put("mon_delta", updated_delta);
                toSendEntry = updatedRecord;

                //System.out.println("To Send:"+toSend.toString());
            }else{
                System.out.println("Key was not in store");
                toSendEntry.put("mon_delta", -1);
            }
            newStoredSnap.put(key,newEntry.getJSONObject(key));

            log.info("Entry Key: "+ key);
            toSend.put(getDataToSend(key,toSendEntry));
        }
        context.putState(prevSnapshotKey, ByteBuffer.wrap(newStoredSnap.toString().getBytes(StandardCharsets.UTF_8)));

        log.info("to Send length: "+ toSend.length() );
       // log.info("Processing Duration: "+(System.currentTimeMillis()-start));
        context.getCurrentRecord().ack();
        return toSend.toString().getBytes(StandardCharsets.UTF_8);
    }

    public JSONObject getDataToSend(String key,JSONObject data){

        if(data.getJSONObject("counters").has("pkg_cache_lookups")) {
            log.info("Delta:" + data.getJSONObject("counters").getLong("pkg_cache_lookups"));
        }

        JSONObject toSendEntry = new JSONObject();
        toSendEntry.put("mon_entry_key", key);
        toSendEntry.put("mon_instance", data.getString("mon_instance"));
        toSendEntry.put("mon_dt", data.getString("mon_dt"));
        toSendEntry.put("mon_delta", data.getLong("mon_delta"));

        JSONObject keyCols = data.getJSONObject("keys");
        JSONObject textStatesCols =  data.getJSONObject("textstates");
        JSONObject statesCols = data.getJSONObject("states");
        JSONObject countersCols = data.getJSONObject("counters");

        for(String col: keyCols.keySet()){
            toSendEntry.put(col,keyCols.getString(col));
        }
        for(String col: textStatesCols.keySet()){
            toSendEntry.put(col,textStatesCols.getString(col));
        }
        for(String col: statesCols.keySet()){
            toSendEntry.put(col,statesCols.getLong(col));
        }
        for(String col: countersCols.keySet()){
            toSendEntry.put(col,countersCols.getLong(col));
        }
        return toSendEntry;
    }

    public JSONObject calcDeltas(JSONObject currRecord, JSONObject prevRecord){

        if(currRecord.has("pkg_cache_lookups") && prevRecord.has("pkg_cache_lookups")) {
            log.info("Curr:" + currRecord.getLong("pkg_cache_lookups")+"    Prev:" + prevRecord.getLong("pkg_cache_lookups"));
        }
        JSONObject deltas =  new JSONObject();
        for(String col: currRecord.keySet()){
            long delta = Math.subtractExact(currRecord.getLong(col), prevRecord.getLong(col));
            deltas.put(col, delta);
        }
        return deltas;
    }

    public JSONObject getPrevSnapshot(Context context, String prevSnapshotKey){
        JSONObject prevSnapshot = null;
        try {
            ByteBuffer storedSanpshot = context.getState(prevSnapshotKey);
            prevSnapshot = new JSONObject(new String(storedSanpshot.array(),StandardCharsets.UTF_8));
        }catch (Exception e){
            e.printStackTrace();
            log.info(e.toString());
            log.info(e.getMessage());
            log.info("There isn't a previous snapshot.");
        }
        return prevSnapshot;
    }

    /**
     * activekeys structure:
     *
     * {
     *    mon_snapshot_id: snap1 //id do snapshot
     *    keys:[ key1, key2, key3, ..] // chaves que identificam cada uma das linhas do snapshot
     * }
     * */
    public JSONObject removeInactiveKeys(Context context,JSONObject prevSnapshot, String prevSnapshotKey){
        log.info("Removing inactive keys");
        JSONObject activeKeys = getActiveKeys(context,prevSnapshotKey);
        JSONObject cleanedPrevSnapshot = new JSONObject();
        if(activeKeys != null ){
            JSONArray keysList = activeKeys.getJSONArray("keys");
            for(int i=0; i<keysList.length(); i++){
                String key = keysList.getString(i);
                cleanedPrevSnapshot.put(key, prevSnapshot.getJSONObject(key));
            }
            context.putState(prevSnapshotKey, ByteBuffer.wrap(cleanedPrevSnapshot.toString().getBytes(StandardCharsets.UTF_8)));
        }
        resetActiveKeys(context, prevSnapshotKey);
        return cleanedPrevSnapshot;
    }

    public JSONObject getActiveKeys(Context context, String prevSnapshotKey){
        JSONObject activeKeys = null;
        //CompletableFuture<ByteBuffer> res =  context.getStateAsync(prevSnapshotKey+"_activekeys");
        try {
            log.info("Getting active keys...");
            //ByteBuffer storedActiveKeys = res.join();
            /*ByteBuffer storedActiveKeys = res.getNow(ByteBuffer.wrap(new byte[0]));
            while(storedActiveKeys.array().length==0){
                log.info("In while loop..");
                res =  context.getStateAsync(prevSnapshotKey+"_activekeys");
                storedActiveKeys = res.getNow(ByteBuffer.wrap(new byte[0]));
            }*/
            String activeKeys_key = prevSnapshotKey+"_activekeys";
            ByteBuffer storedActiveKeys = context.getState(activeKeys_key);
            log.info("Activekeys ready");
            activeKeys = new JSONObject(new String(storedActiveKeys.array(), StandardCharsets.UTF_8));

        }catch (Exception e){
            e.printStackTrace();
            log.info(e.toString());
            log.info(e.getMessage());

            /*if(res.isCancelled()){
                log.info("Get keys operation was cancelled!");
            }*/
            log.info("There aren't any active keys stored.");
        }
        return activeKeys ;
    }

    public void resetActiveKeys(Context context, String prevSnapshotKey){
        log.info("Reseting activekeys...");
        JSONObject obj = new JSONObject();
        obj.put("mon_snapshot_id", "");
        obj.put("keys", new JSONArray());
        context.putState(prevSnapshotKey+"_activekeys", ByteBuffer.wrap(obj.toString().getBytes(StandardCharsets.UTF_8)));
    }


    public void addToActiveKeys(Context context, String prevSnapshotKey, String key, String curr_snap_id){
        JSONObject activeKeys = getActiveKeys(context, prevSnapshotKey);
        if (activeKeys != null){
            activeKeys.getJSONArray("keys").put(key);
            if(activeKeys.getString("mon_snapshot_id").equals("")){
                activeKeys.put("mon_snapshot_id",curr_snap_id);
            }
            context.putState(prevSnapshotKey+"_activekeys", ByteBuffer.wrap(activeKeys.toString().getBytes(StandardCharsets.UTF_8)));
        }
    }

    /*
    public JSONObject removeInactiveKeys(Context context,JSONObject prevSnapshot, JSONObject newEntry, String entryKey){
        ByteBuffer storedActiveKeys = null;
        JSONObject activeKeys = null;
        try {
            storedActiveKeys = context.getState("activekeys");
            activeKeys = new JSONObject(new String(storedActiveKeys.array()));
        }catch (Exception e){
            log.info("There aren't active keys stored.");
        }

        String snapshot_id = newEntry.getString("mon_snapshot_id");
        if(activeKeys == null || activeKeys.keySet().contains(snapshot_id)){
            JSONArray keyList = new JSONArray();
            keyList.put(entryKey);
            JSONObject o =  new JSONObject();
            o.put("snapshot_time",newEntry.getLong("mon_dt")); //mon_dt tem o momento em que foi tirado o snapshot
            o.put("keys", keyList);
            activeKeys.put(snapshot_id, o);
        }

        if(activeKeys.keySet().size() >2){

            HashMap<Long, String> times = new HashMap<>();
            for(String snap_id : activeKeys.keySet()){
                Long snap_time = activeKeys.getJSONArray(snap_id).getJSONObject(0).getLong("snap_time");
                times.put(snap_time, snap_id);
            }

            List<Long> times_list = new ArrayList<>(times.keySet());
            Collections.sort(times_list);

            String mostRecentSnap = times.get(times_list.get(0));
            String secondMostRecentSnap = times.get(times_list.get(1));

            JSONObject cleanedPrevSnapshot = new JSONObject();

            JSONArray mostRecentKeys = activeKeys.getJSONObject(mostRecentSnap).getJSONArray("keys");
            for( int i =0; i<mostRecentKeys.length(); i++) {
                String key = mostRecentKeys.getString(i);
                cleanedPrevSnapshot.put(key, prevSnapshot.getJSONObject(key));
            }

            JSONArray secondRecentKeys = activeKeys.getJSONObject(secondMostRecentSnap).getJSONArray("keys");
            for( int i =0; i<secondRecentKeys.length(); i++) {
                String key = mostRecentKeys.getString(i);
                cleanedPrevSnapshot.put(key, prevSnapshot.getJSONObject(key));
            }

           return cleanedPrevSnapshot;
        }
        return null;

    }

     */
}
