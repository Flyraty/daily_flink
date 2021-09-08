### 验证 WatermarkStrategy

主要验证以下几个问题，以下的测试都是基于单并行度的，不涉及 watermark 的合并，只要一个 watermark 到了，就会触发整个窗口内所有 key 的计算。
1. 何时会触发生成 watermark？
2. 窗口计算是左闭右开还是其他，一个 10s 的固定窗口，19999 是否会触发计算？
3. watermark 的定时生成和规则生成？

#### WatermarkStrategyExample

命令行输入 `nc -l 9000`。执行 WatermarkStrategyExample 程序。该程序使用的是内置 BoundedOutOfOrderness WatermarkGenerator，10s 的固定窗口和 3s 的延迟。下面开始在命令中输入数据

```sh
$ nc -l 9900
10000 a
11000 a
15000 a
16000 b
20000 b
14000 b
22999 a
23000 a
```

对应的 flink 程序输出结果如下。

```sh
systemTime:1631091182974, Event:10000 a
systemTime:1631091197858, Event:11000 a
systemTime:1631091231381, Event:15000 a
systemTime:1631091272747, Event:16000 b
systemTime:1631091291263, Event:20000 b
systemTime:1631091312097, Event:14000 b
systemTime:1631091326017, Event:22999 a
systemTime:1631091357116, Event:23000 a
6> (a,3)
2> (b,2)
``` 
可以看到在 23000 的时候触发了 10000 ~ 20000 这个 10s 窗口的计算。计算的结果 b=2，这意味着 `20000 b` 这条数据没有在该窗口内。由此得出结论，窗口是左闭右开的，比如 10000~19999 会参与计算，因为固定延迟 3s，所以是 `19999+3000`会
触发第一个窗口的计算。等下，看输出不是在 23000 的时候触发的计算吗，这里就要看下 BoundedOutOfOrdernessWatermarks 的实现，23000 的时候生成的 watermark 其实是 22999，做了 -1 操作。
```java
public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {

    /** The maximum timestamp encountered so far. */
    private long maxTimestamp;

    /** The maximum out-of-orderness that this watermark generator assumes. */
    private final long outOfOrdernessMillis;

    /**
     * Creates a new watermark generator with the given out-of-orderness bound.
     *
     * @param maxOutOfOrderness The bound for the out-of-orderness of the event timestamps.
     */
    public BoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness) {
        checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
        checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");

        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

        // start so that our lowest watermark would be Long.MIN_VALUE.
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    // ------------------------------------------------------------------------

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
    }
}
```

#### PeriodicWatermarkGeneratorExample
Flink 1.11 之后重构了 watermark 的接口。这里自定义实现了上述 BoundedOutOfOrdernessWatermarks。启动 PeriodicWatermarkGeneratorExample，重新输入上面的数据，输出结果和上面也是一致的。

```sh
systemTime:1631092875427, Event:10000 a
systemTime:1631092875427, Event:11000 a
systemTime:1631092875427, Event:15000 a
systemTime:1631092875427, Event:16000 b
systemTime:1631092875427, Event:20000 b
systemTime:1631092875428, Event:14000 b
systemTime:1631092875428, Event:22999 a
systemTime:1631092876190, Event:23000 a
6> (a,3)
2> (b,2)
```

#### PunctuatedWatermarkGeneratorExample
基于规则实现，这里是只要 a，b 出现就会触发 watermark 生成。10s 的固定窗口，不处理延迟数据
```java
private static class MyPunctuatedWatermarkGenerator implements WatermarkGenerator<String> {

        private List<String> monitorEvents = Arrays.asList("a", "b");

        @Override
        public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
            String eventMarker = event.split(" ")[1];
            if (monitorEvents.contains(eventMarker)) {
                long eventTimestamps = Long.parseLong(event.split(" ")[0]);
                Watermark watermark = new Watermark(eventTimestamps);
                System.out.println(String.format("systemTime:%s, Event:%s", Long.toString(System.currentTimeMillis()), event));
                output.emitWatermark(watermark);
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }
```
下面执行下 PunctuatedWatermarkGeneratorExample，命令行输入

```bash
$ nc -l 9902
10000 a
19999 b
20000 a
25000 b
30000 b
31000 b
39999 c
40000 a
```
查看程序输出，可以看到 `39999 c` 并不会触发 30000~39999 的窗口计算。
```bash
systemTime:1631094084976, Event:10000 a
systemTime:1631094101924, Event:19999 b
6> (a,1)
2> (b,1)
systemTime:1631094117289, Event:20000 a
systemTime:1631094123379, Event:25000 b
systemTime:1631094134345, Event:30000 b
2> (b,1)
6> (a,1)
systemTime:1631094153463, Event:31000 b
systemTime:1631094170745, Event:40000 a
4> (c,1)

```

#### 总结
基于以上测试可以回答一开始踢出的问题。
1. watermark 的生成有两种方式，定时和规则，定时生成的间隔可以通过 `end.setAutoWatermarkInterval()` 来设置，默认值如下。规则生成是只要符合规则就会生成 watermark。

```java
 public void setStreamTimeCharacteristic(TimeCharacteristic characteristic) {
        this.timeCharacteristic = Preconditions.checkNotNull(characteristic);
        if (characteristic == TimeCharacteristic.ProcessingTime) {
            getConfig().setAutoWatermarkInterval(0);
        } else {
            getConfig().setAutoWatermarkInterval(200);
        }
    }
```

2. 窗口是左闭右开，至于类似于 19999 这种时间是否会触发计算取决于 watermarkGenerator 的实现。
3. 通过测试也可以看出来，基于规则生成很可能产生连续的 watermark，频繁的触发计算可能会降低性能。





