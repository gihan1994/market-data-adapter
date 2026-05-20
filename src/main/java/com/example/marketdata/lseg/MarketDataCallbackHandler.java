package com.example.marketdata.lseg;

import com.refinitiv.ema.access.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Bridges EMA callbacks → internal {@link MarketDataTick} stream.
 *
 * EMA invokes these methods from its dispatcher thread; do NOT block here.
 * Listeners should hand off via executor if any heavy work is needed.
 */
@Component
@Slf4j
public class MarketDataCallbackHandler implements OmmConsumerClient {

    // Common LSEG field IDs (FIDs) from RDM Field Dictionary
    private static final int FID_BID    = 22;   // BID
    private static final int FID_ASK    = 25;   // ASK
    private static final int FID_TRDPRC = 6;    // TRDPRC_1 (last trade)
    private static final int FID_ACVOL  = 32;   // ACVOL_1  (accumulated volume)
    private static final int FID_TIMACT = 5;    // TIMACT   (time of activity)

    private final CopyOnWriteArrayList<TickListener> listeners = new CopyOnWriteArrayList<>();

    public void addListener(TickListener l) { listeners.add(l); }
    public void removeListener(TickListener l) { listeners.remove(l); }

    @Override
    public void onRefreshMsg(RefreshMsg msg, OmmConsumerEvent event) {
        try {
            String ric = msg.hasName() ? msg.name() : "?";
            MarketDataTick tick = extract(msg.payload(), ric, MarketDataTick.Type.SNAPSHOT);
            tick.setComplete(msg.complete());
            dispatch(tick);
        } catch (Exception e) {
            log.error("onRefreshMsg failed", e);
        }
    }

    @Override
    public void onUpdateMsg(UpdateMsg msg, OmmConsumerEvent event) {
        try {
            String ric = msg.hasName() ? msg.name() : "?";
            MarketDataTick tick = extract(msg.payload(), ric, MarketDataTick.Type.UPDATE);
            dispatch(tick);
        } catch (Exception e) {
            log.error("onUpdateMsg failed", e);
        }
    }

    @Override
    public void onStatusMsg(StatusMsg msg, OmmConsumerEvent event) {
        String ric = msg.hasName() ? msg.name() : "?";
        String stateText = msg.hasState() ? msg.state().toString() : "no-state";
        log.info("StatusMsg ric={} state={}", ric, stateText);
        MarketDataTick tick = MarketDataTick.builder()
                .ric(ric)
                .type(MarketDataTick.Type.STATUS)
                .statusText(stateText)
                .receivedAt(Instant.now())
                .build();
        dispatch(tick);
    }

    @Override public void onGenericMsg(GenericMsg msg, OmmConsumerEvent event) {}
    @Override public void onAckMsg(AckMsg msg, OmmConsumerEvent event) {}
    @Override public void onAllMsg(Msg msg, OmmConsumerEvent event) {}

    // --------- helpers ---------

    /**
     * Extract tick fields from an EMA Payload (which wraps a FieldList for OMM messages).
     */
    private MarketDataTick extract(Payload payload, String ric, MarketDataTick.Type type) {
        Map<String, Object> extras = new HashMap<>();
        MarketDataTick.MarketDataTickBuilder b = MarketDataTick.builder()
                .ric(ric)
                .type(type)
                .receivedAt(Instant.now());

        if (payload != null && payload.dataType() == DataType.DataTypes.FIELD_LIST) {
            FieldList fl = payload.fieldList();
            for (FieldEntry fe : fl) {
                try {
                    switch (fe.fieldId()) {
                        case FID_BID    -> b.bid(toBigDecimal(fe));
                        case FID_ASK    -> b.ask(toBigDecimal(fe));
                        case FID_TRDPRC -> b.last(toBigDecimal(fe));
                        case FID_ACVOL  -> b.volume(fe.uintValue());
                        default -> {
                            // capture a few field types for downstream filtering
                            int lt = fe.loadType();
                            if (lt == DataType.DataTypes.REAL
                                    || lt == DataType.DataTypes.INT
                                    || lt == DataType.DataTypes.DOUBLE) {
                                extras.put(String.valueOf(fe.fieldId()), fe.load().toString());
                            }
                        }
                    }
                } catch (Exception ignored) { /* swallow per-field errors */ }
            }
        }
        return b.extra(extras).build();
    }

    private BigDecimal toBigDecimal(FieldEntry fe) {
        int lt = fe.loadType();
        if (lt == DataType.DataTypes.REAL) {
            return BigDecimal.valueOf(fe.real().asDouble());
        }
        if (lt == DataType.DataTypes.DOUBLE) {
            return BigDecimal.valueOf(fe.doubleValue());
        }
        return null;
    }

    private void dispatch(MarketDataTick tick) {
        for (TickListener l : listeners) {
            try { l.onTick(tick); }
            catch (Exception e) { log.error("Tick listener failed for ric={}", tick.getRic(), e); }
        }
    }
}
