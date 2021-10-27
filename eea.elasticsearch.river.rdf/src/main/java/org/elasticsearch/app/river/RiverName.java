package org.elasticsearch.app.river;

import java.io.Serializable;

/**
 *
 */
public class RiverName implements Serializable {

    private final String type;

    private final String name;

    public RiverName(String type, String name) {
        this.type = type;
        this.name = name;
    }


    public String type() {
        return this.type;
    }

    public String getType() {
        return type();
    }

    public String name() {
        return this.name;
    }

    public String getName() {
        return name();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RiverName that = (RiverName) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }
}

