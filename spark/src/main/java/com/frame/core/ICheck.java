package com.frame.core;

/**
 * 校验接口
 *
 * @author XiaShuai on 2020/4/8.
 */
public interface ICheck {
    /**
     * 校验对象信息严格性
     *
     * @return true&false
     */
    default boolean check() {
        return true;
    }

}
