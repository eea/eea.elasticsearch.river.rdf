package org.elasticsearch.app.api.server.entities;

import javax.persistence.*;
import java.util.Date;

@Entity
public class UpdateRecord {

    @Id
    @GeneratedValue
    private long id;

    @Column(nullable = false)
    @Basic(optional = false)
    private Date lastUpdateStartDate;

    @Column(nullable = false)
    @Basic(optional = false)
    private long lastUpdateDuration;

    @Column(nullable = false)
    @Basic(optional = false)
    private UpdateStates finishState;

    @Column(nullable = false)
    @Basic(optional = false)
    private long indexedESHits;

    public UpdateRecord() {
        finishState = UpdateStates.FAILED;
        this.lastUpdateStartDate = new Date(System.currentTimeMillis());
        indexedESHits=0;
    }

    public UpdateRecord(Date lastUpdateStartDate, long lastUpdateDuration) {
        super();
        this.lastUpdateStartDate = lastUpdateStartDate;
        this.lastUpdateDuration = lastUpdateDuration;
    }

    public long getId() {
        return id;
    }

    public Date getLastUpdateStartDate() {
        return lastUpdateStartDate;
    }

    public void setLastUpdateStartDate(Date lastUpdateDate) {
        this.lastUpdateStartDate = lastUpdateDate;
    }

    public long getLastUpdateDuration() {
        return lastUpdateDuration;
    }

    public void setLastUpdateDuration(long lastUpdateDuration) {
        this.lastUpdateDuration = lastUpdateDuration;
    }

    public UpdateStates getFinishState() {
        return finishState;
    }

    public void setFinishState(UpdateStates state) {
        this.finishState = state;
    }

    public long getIndexedESHits() {
        return indexedESHits;
    }

    public void addToIndexedESHits(long indexedESHits) {
        this.indexedESHits += indexedESHits;
    }
}
