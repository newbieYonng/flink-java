package state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;

public class IncomeCountFunction extends RichMapFunction<Tuple3<String, String, Double>, Tuple4<String, Double, String, Double>> {

    private MapState<String, Double> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Double> mapStateDescriptor = new MapStateDescriptor<>("map stste", String.class, Double.class);
        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public Tuple4<String, Double, String, Double> map(Tuple3<String, String, Double> in) throws Exception {
        String province = in.f0;
        String city = in.f1;
        Double money = in.f2;

        Double hisMoney = mapState.get(city);
        if (hisMoney == null)
            hisMoney = 0.0;
        hisMoney += money;
        mapState.put(city, hisMoney);

        Double totalMonry = 0.0;
        Iterable<Double> hisTotalMoneys = mapState.values();
        for (Double hisTotalMoney : hisTotalMoneys) {
            totalMonry += hisTotalMoney;
        }
        return Tuple4.of(province, totalMonry, city, hisMoney);
    }
}
