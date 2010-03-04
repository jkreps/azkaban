package azkaban.common.utils;

/**
 * A helper class for building props
 * 
 * @author jkreps
 * 
 */
public class PropsBuilder {

    Props _parent;
    Props _props;

    public PropsBuilder() {
        this(null);
    }

    public PropsBuilder(Props parent) {
        _parent = parent;
        _props = new Props(parent);
    }

    public PropsBuilder with(String key, String value) {
        _props.put(key, value);
        return this;
    }

    public PropsBuilder with(String key, int value) {
        _props.put(key, value);
        return this;
    }

    public PropsBuilder with(String key, double value) {
        _props.put(key, value);
        return this;
    }

    public PropsBuilder with(String key, long value) {
        _props.put(key, value);
        return this;
    }

    public Props create() {
        Props p = _props;
        _props = new Props(_parent);
        return p;
    }

}
