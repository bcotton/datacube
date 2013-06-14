/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.bucketers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.Bucketer;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.ops.LongOp;
import com.urbanairship.datacube.serializables.LongSerializable;
import org.joda.time.DateTime;

import java.util.List;

/**
 * Narrow a DateTime to a minute interval, e.g. every 5 minutes
 */
public class SecondPeriodBucketer implements Bucketer<DateTime> {
    private final static List<BucketType> bucketTypes = ImmutableList.of(BucketType.IDENTITY);
    private final int interval;

    public SecondPeriodBucketer(int secondInterval) {
        super();
        if (secondInterval < 1 || secondInterval > 60) {
            throw new IllegalArgumentException(this.getClass().getSimpleName() + ": intervals should be between 1 and 60");
        }
        this.interval = secondInterval;
    }

    @Override
    public SetMultimap<BucketType,CSerializable> bucketForWrite(DateTime coordinate) {
        return ImmutableSetMultimap.<BucketType,CSerializable>of(
                BucketType.IDENTITY, new LongOp(getBucket(coordinate, interval)));
    }

    @Override
    public CSerializable bucketForRead(Object coordinate, BucketType bucketType) {
        assert bucketType == BucketType.IDENTITY;
        return new LongSerializable(getBucket((DateTime) coordinate, interval));
    }

    @Override
    public List<BucketType> getBucketTypes() {
        return bucketTypes;
    }

    public static long getBucket(DateTime coordinate, int interval) {
        int roundSeconds = (coordinate.getSecondOfMinute() / interval) * interval;
        DateTime bucketDT = coordinate.minuteOfHour()
            .roundFloorCopy()
            .plusSeconds(roundSeconds);

        return bucketDT.withMillisOfSecond(0).getMillis();
    }
}
