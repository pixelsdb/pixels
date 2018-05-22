package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.stats.ColumnStats;
import cn.edu.ruc.iir.pixels.core.stats.StatsRecorder;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * pixels
 *
 * @author guodong
 */
public class TestPixelsPredicate
{
    private static final BigintType BIGINT = BigintType.BIGINT;
    private static final VarcharType VARCHAR = VarcharType.VARCHAR;
    private static final String COLUMN_A_NAME = "a";
    private static final String COLUMN_B_NAME = "b";
    private static final int COLUMN_A_ORDINAL = 0;
    private static final int COLUMN_B_ORDINAL = 1;
    private static final long TEST_INT = 1L;
    private static final String TEST_VARCHAR = "this";

    @Test
    public void testIntPredicate()
    {
        Domain testingColumnDomain = Domain.singleValue(BIGINT, TEST_INT);
        TupleDomain.ColumnDomain<String> cola = new TupleDomain.ColumnDomain<>(COLUMN_A_NAME, testingColumnDomain);

        TupleDomain<String> effectivePredicate = TupleDomain.fromColumnDomains(Optional.of(ImmutableList.of(cola)));
        TupleDomain<String> emptyEffectivePredicate = TupleDomain.all();

        List<TupleDomainPixelsPredicate.ColumnReference<String>> columnReferences = ImmutableList.<TupleDomainPixelsPredicate.ColumnReference<String>>builder()
                .add(new TupleDomainPixelsPredicate.ColumnReference<>(COLUMN_A_NAME, COLUMN_A_ORDINAL, BIGINT))
                .build();

        TupleDomainPixelsPredicate<String> predicate = new TupleDomainPixelsPredicate<>(effectivePredicate, columnReferences);
        TupleDomainPixelsPredicate<String> emptyPredicate = new TupleDomainPixelsPredicate<>(emptyEffectivePredicate, columnReferences);

        TypeDescription typeDescription = TypeDescription.createInt();
        StatsRecorder statsRecorder = StatsRecorder.create(typeDescription);
        statsRecorder.updateInteger(TEST_INT, 1);
        Map<Integer, ColumnStats> matchingStatsMap = ImmutableMap.of(0, statsRecorder, 1, statsRecorder);

        statsRecorder = StatsRecorder.create(typeDescription);
        statsRecorder.updateInteger(100L, 1);
        Map<Integer, ColumnStats> unMatchingStatsMap = ImmutableMap.of(0, statsRecorder);

        assertTrue(predicate.matches(1L, matchingStatsMap));
        assertTrue(emptyPredicate.matches(1L, matchingStatsMap));
        assertFalse(predicate.matches(1L, unMatchingStatsMap));
    }

    @Test
    public void testVarcharPredicate()
    {
        Domain testingColumnDomain = Domain.singleValue(VARCHAR, TEST_VARCHAR);
        TupleDomain.ColumnDomain<String> colb = new TupleDomain.ColumnDomain<>(COLUMN_B_NAME, testingColumnDomain);

        TupleDomain<String> effectivePredicate = TupleDomain.fromColumnDomains(Optional.of(ImmutableList.of(colb)));
        TupleDomain<String> emptyEffectivePredicate = TupleDomain.all();

        List<TupleDomainPixelsPredicate.ColumnReference<String>> columnReferences = ImmutableList.<TupleDomainPixelsPredicate.ColumnReference<String>>builder()
                .add(new TupleDomainPixelsPredicate.ColumnReference<>(COLUMN_B_NAME, COLUMN_B_ORDINAL, VARCHAR))
                .build();

        TupleDomainPixelsPredicate<String> predicate = new TupleDomainPixelsPredicate<>(effectivePredicate, columnReferences);
        TupleDomainPixelsPredicate<String> emptyPredicate = new TupleDomainPixelsPredicate<>(emptyEffectivePredicate, columnReferences);

        TypeDescription typeDescription = TypeDescription.createVarchar();
        StatsRecorder statsRecorder = StatsRecorder.create(typeDescription);
        statsRecorder.updateString(TEST_VARCHAR, 1);
        Map<Integer, ColumnStats> matchingStatsMap = ImmutableMap.of(0, statsRecorder, 1, statsRecorder);

        statsRecorder = StatsRecorder.create(typeDescription);
        statsRecorder.updateString("a", 1);
        Map<Integer, ColumnStats> unMatchingStatsMap = ImmutableMap.of(0, statsRecorder);

        assertTrue(predicate.matches(1L, matchingStatsMap));
        assertTrue(emptyPredicate.matches(1L, matchingStatsMap));
        assertFalse(predicate.matches(1L, unMatchingStatsMap));
    }
}
