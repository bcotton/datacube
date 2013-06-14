/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.bucketers.MinutePeriodBucketer;
import com.urbanairship.datacube.bucketers.SecondPeriodBucketer;
import com.urbanairship.datacube.bucketers.StringToBytesBucketer;
import com.urbanairship.datacube.dbharnesses.MapDbHarness;
import com.urbanairship.datacube.idservices.CachingIdService;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class SecondPeriodTest {

    static class HostSecondCube {
        Dimension<DateTime> time = new Dimension<DateTime>("time", new SecondPeriodBucketer(5), false, 8);
        Dimension<String> host = new Dimension<String>("host", new StringToBytesBucketer(), true, 5);
        List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(time, host);

        Rollup hostRollup = new Rollup(host);
        Rollup hostIntervalRollup = new Rollup(host, time);

        List<Rollup> rollups = ImmutableList.of(hostRollup, hostIntervalRollup);

        DataCube<LongOp> dataCube = new DataCube<LongOp>(dimensions, rollups);
        ConcurrentMap<BoxedByteArray,byte[]> backingMap = Maps.newConcurrentMap();
        IdService idService = new CachingIdService(4, new MapIdService(),"test");
        DbHarness<LongOp> dbHarness = new MapDbHarness<LongOp>(backingMap,
                LongOp.DESERIALIZER, CommitType.READ_COMBINE_CAS, idService);
        DataCubeIo<LongOp> dataCubeIo = new DataCubeIo<LongOp>(dataCube, dbHarness, 1,
                Long.MAX_VALUE, SyncLevel.FULL_SYNC);

        public void addEvent(String hostName, DateTime dateTime) throws IOException, InterruptedException {
            dataCubeIo.writeSync(new LongOp(1), new WriteBuilder(dataCube)
                .at(time, dateTime)
                .at(host, hostName));
        }

        public long getHostCount(String hostName) throws IOException, InterruptedException {
            Optional<LongOp> opt = dataCubeIo.get(new ReadBuilder(dataCube)
                .at(host, hostName));
            if(!opt.isPresent()) {
                return 0;
            } else {
                return opt.get().getLong();
            }
        }

        public long getHostCountBySecond(String hostName, DateTime dateTime) throws IOException, InterruptedException {
            Optional<LongOp> opt = dataCubeIo.get(new ReadBuilder(dataCube)
                .at(host, hostName)
                .at(time, dateTime));
            if(!opt.isPresent()) {
                return 0;
            } else {
                return opt.get().getLong();
            }
        }
    };

    @Test
    public void test() throws Exception {
        HostSecondCube hostMinCube = new HostSecondCube();

        DateTime now = new DateTime(DateTimeZone.UTC);
        DateTime topOfHour = now.minuteOfHour().roundFloorCopy();
        hostMinCube.addEvent("host1", topOfHour);
        hostMinCube.addEvent("host1", topOfHour.plusSeconds(6));
        hostMinCube.addEvent("host1", topOfHour.plusSeconds(6));
        hostMinCube.addEvent("host1", topOfHour.plusSeconds(6));

        Assert.assertEquals(1, hostMinCube.getHostCountBySecond("host1", topOfHour));

        Assert.assertEquals(3, hostMinCube.getHostCountBySecond("host1", topOfHour.plusSeconds(5)));
        Assert.assertEquals(0, hostMinCube.getHostCountBySecond("host1", topOfHour.plusSeconds(10)));

        Assert.assertEquals(4, hostMinCube.getHostCount("host1"));
    }

    @Test
    public void testGetBucket() {
        for(int i = 0; i < 5; i++) {
            Assert.assertEquals(
                    SecondPeriodBucketer.getBucket(new DateTime(2013, 06, 13, 12, 0, i, 0), 5),
            new DateTime(2013, 06, 13, 12, 0, 0, 0).getMillis());
        }
    }
}
