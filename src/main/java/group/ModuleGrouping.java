package group;


import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * User: Paul Zhang
 * Date: 15/3/27
 * Time: 下午5:01
 */
public class ModuleGrouping implements CustomStreamGrouping, Serializable {

    int numTasks = 0;

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        numTasks = list.size();
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
        List<Integer> boltIds = new ArrayList<>();

        System.out.println("i===================== " + i);
        System.out.println("size================== " + list.size());
        if (list.size() > 0) {
            String str = list.get(0).toString();
            System.out.println("str===================" + str);
            if (str.isEmpty()) {
                boltIds.add(0);
            } else {
                boltIds.add(str.charAt(0) % numTasks);
            }
        }
        return boltIds;
    }
}
