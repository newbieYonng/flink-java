package state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;

import java.util.HashSet;

public class AdCountFunction extends RichMapFunction<Tuple3<String, String, String>, Tuple4<String, String, Integer, Integer>> {

    private ValueState<Integer> pvState;
    private ValueState<HashSet<String>> uvState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> pv_stateDescriptor = new ValueStateDescriptor<>("PV STATE", Integer.class);
        pvState = getRuntimeContext().getState(pv_stateDescriptor);

        ValueStateDescriptor<HashSet<String>> uv_stateDescriptor = new ValueStateDescriptor<>("UV STATE", TypeInformation.of(new TypeHint<HashSet<String>>() {}));
        uvState = getRuntimeContext().getState(uv_stateDescriptor);
    }

    @Override
    public Tuple4<String, String, Integer, Integer> map(Tuple3<String, String, String> in) throws Exception {
        String aid = in.f0;
        String uid = in.f1;
        String eventType = in.f2;

        //pv
        Integer hisCount = pvState.value();
        if (hisCount == null)
            hisCount = 0;
        hisCount += 1;
        pvState.update(hisCount);

        //uv
        HashSet<String> uidSet = uvState.value();
        if (uidSet == null)
            uidSet = new HashSet<>();
        uidSet.add(uid);
        uvState.update(uidSet);

        return Tuple4.of(aid, eventType, hisCount, uidSet.size());
    }
}
