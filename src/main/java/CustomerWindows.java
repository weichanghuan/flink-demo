import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.*;
import java.util.concurrent.TimeUnit;


public class CustomerWindows extends WindowAssigner<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;


    private final long offset;

    private final long slide;

    private CustomerWindows(long slide, long offset) {
        if (Math.abs(offset) >= slide) {
            throw new IllegalArgumentException("CustomerWindows parameters must satisfy " +
                    "abs(offset) < slide and size > 0");
        }

        this.slide = slide;
        this.offset = offset;
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        int s = (int)(1+Math.random()*(10-1+1));
        long size = s*1000L;// 最近6个月天数(不包括当月)

        System.out.println("size："+size);
        timestamp = context.getCurrentProcessingTime();
        List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
        long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
        for (long start = lastStart;
             start > timestamp - size;
             start -= slide) {
            windows.add(new TimeWindow(start, start + size));
        }
        return windows;
    }


    //java获取当前月的天数
    public static int getDayOfMonth(int m){
        Calendar cal = Calendar.getInstance(Locale.CHINA);
        cal.add(Calendar.MONTH, m);
        int day=cal.getActualMaximum(Calendar.DATE);
        return day;
    }


    public long getSlide() {
        return slide;
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return ProcessingTimeTrigger.create();
    }

    @Override
    public String toString() {
        return "CustomerWindows(" + slide + ")";
    }

    public static CustomerWindows of(Time slide) {
        return new CustomerWindows(slide.toMilliseconds(), 0);
    }

    public static CustomerWindows of(Time slide, Time offset) {
        return new CustomerWindows(slide.toMilliseconds(), offset.toMilliseconds());
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
}
