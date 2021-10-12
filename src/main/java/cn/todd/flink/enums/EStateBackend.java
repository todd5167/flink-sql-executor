package cn.todd.flink.enums;

import org.apache.commons.lang3.StringUtils;

/**
 * Date: 2021/10/01
 *
 * @author todd5167
 */
public enum EStateBackend {
    /** memory */
    MEMORY,
    /** rockdb */
    ROCKSDB,
    /** filesystem */
    FILESYSTEM;

    public static EStateBackend convertFromString(String type) {
        if (StringUtils.isEmpty(type)) {
            throw new RuntimeException("null StateBackend!");
        }
        return valueOf(type.toUpperCase());
    }
}
