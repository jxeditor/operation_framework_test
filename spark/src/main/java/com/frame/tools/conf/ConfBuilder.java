package com.frame.tools.conf;

import com.frame.tools.internal.Loggable;
import com.frame.tools.lang.Nullable;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Github> https://github.com/typesafehub/config
 *
 * @author XiaShuai on 2020/4/8.
 */
public class ConfBuilder implements Loggable {
    /**
     * 输出 config 内容为字符串相关配置
     * 主要包括是否添加注释、格式化、是否为 json 格式
     * https://www.programcreek.com/scala/com.typesafe.config.ConfigRenderOptions
     */
    static final ConfigRenderOptions CONFIG_RENDER_OPTIONS_JSON = ConfigRenderOptions.defaults()
            .setComments(false)
            .setOriginComments(false)
            .setFormatted(true)
            .setJson(true);
    private static final String SYSTEM_ENV_VAL_NAME_FORMAT = "%env";

    /**
     * 固定前缀字段
     * eg. usePrefix=conf
     * 则最终解析后仅返回以 conf 开头变量
     */
    @Nullable
    private String usePrefix;
    /**
     * 使用环境变量名称替换路径参数
     * 提取环境变量名称优先级 System.getProperties > System.getenv
     * eg.
     * path=/conf/%env/app.conf , systemEnvValName=env env=dev , 结果为 path=/conf/dev/app.conf
     * path=/conf/app-%env.conf , systemEnvValName=env env=dev , 结果为 path=/conf/app-dev.conf
     *
     * @see #buildEnvPath(String)
     */
    @Nullable
    private String systemEnvValName;
    /**
     * 私有化
     */
    private Config config = ConfigFactory.empty();
    /**
     * 是否完成构建
     */
    private boolean build = false;

    private ConfBuilder() {
    }

    private ConfBuilder(String usePrefix) {
        this.usePrefix = usePrefix;
    }

    private ConfBuilder(String usePrefix, String systemEnvValName) {
        this.usePrefix = usePrefix;
        this.systemEnvValName = systemEnvValName;
        logInfo("Loading configs first row is highest priority, second row is fallback and so on");
    }

    public static ConfBuilder create() {
        return new ConfBuilder();
    }

    public static ConfBuilder create(String usePrefix) {
        return new ConfBuilder(usePrefix);
    }

    public static ConfBuilder create(String usePrefix, String systemEnvValName) {
        return new ConfBuilder(usePrefix, systemEnvValName);
    }

    protected Config getConfig() {
        return this.config;
    }

    /**
     * 路径内容替换
     * 提换逻辑参考 {@link #systemEnvValName} 详细描述
     *
     * @param path path
     * @return path
     */
    private String buildEnvPath(String path) {
        if (systemEnvValName == null) {
            return path;
        }
        final String p = System.getProperty(systemEnvValName);
        final String e = System.getenv(systemEnvValName);
        if (p == null && e == null) {
            throw new NullPointerException("systemEnvValName " + systemEnvValName + " null");
        }

        if (!path.contains(SYSTEM_ENV_VAL_NAME_FORMAT)) {
            throw new IllegalArgumentException("path not found " + SYSTEM_ENV_VAL_NAME_FORMAT + " , path:" + path);
        }
        return p == null ?
                path.replaceAll(SYSTEM_ENV_VAL_NAME_FORMAT, e) :
                path.replaceAll(SYSTEM_ENV_VAL_NAME_FORMAT, p);
    }

    /**
     * Resource 文件
     *
     * @param resource the resource
     * @return the builder
     */
    public ConfBuilder withFallbackResource(String resource) {
        final Config resourceConfig = ConfigFactory.parseResources(this.buildEnvPath(resource));
        final String empty = resourceConfig.entrySet().isEmpty() ? " contains no values" : "";
        this.config = this.config.withFallback(resourceConfig);
        logInfo("Loaded config file from resource ({}){}", resource, empty);
        return this;
    }

    /**
     * string/json 字符串.
     *
     * @param str the str
     * @return the config helper
     */
    public ConfBuilder withFallbackString(String str) {
        final Config mapConfig = ConfigFactory.parseString(str);
        final String empty = mapConfig.entrySet().isEmpty() ? " contains no values" : "";
        this.config = this.config.withFallback(mapConfig);
        logInfo("Loaded config file from str ({}){}", str, empty);
        return this;
    }

    /**
     * map 格式数据.
     *
     * @param values the values
     * @return the config helper
     */
    public ConfBuilder withFallbackMap(Map<String, Object> values) {
        final Config mapConfig = ConfigFactory.parseMap(values);
        final String empty = mapConfig.entrySet().isEmpty() ? " contains no values" : "";
        this.config = this.config.withFallback(mapConfig);
        logInfo("Loaded config file from Map ({}){}", values, empty);
        return this;
    }

    /**
     * 系统参数.
     *
     * @return the builder
     */
    public ConfBuilder withFallbackSystemProperties() {
        this.config = this.config.withFallback(Configs.systemProperties());
        logInfo("Loaded system properties into config");
        return this;
    }

    /**
     * 系统环境变量参数.
     *
     * @return the builder
     */
    public ConfBuilder withFallbackSystemEnvironment() {
        this.config = this.config.withFallback(Configs.systemEnvironment());
        logInfo("Loaded system environment into config");
        return this;
    }

    /**
     * 指定路径配置文件
     *
     * @param path the path
     * @return the builder
     */
    public ConfBuilder withFallbackOptionalFile(String path) {
        final File secureConfFile = new File(this.buildEnvPath(path));
        if (secureConfFile.exists()) {
            logInfo("Loaded config file from path ({})", path);
            this.config = this.config.withFallback(ConfigFactory.parseFile(secureConfFile));
        } else {
            logInfo("Attempted to load file from path ({}) but it was not found", path);
        }
        return this;
    }

    /**
     * 相对路径配置文件
     *
     * @param path the path
     * @return the builder
     */
    public ConfBuilder withFallbackOptionalRelativeFile(String path) {
        return this.withFallbackOptionalFile(Configs.getExecutionDirectory() + path);
    }

    /**
     * 配置
     *
     * @param config the config
     * @return the builder
     */
    private ConfBuilder withFallbackConfig(Config config) {
        this.config = this.config.withFallback(config);
        return this;
    }

    /**
     * 构建最终配置.
     * resolve > filter usePrefix
     */
    public synchronized Conf build() {

        if (this.build) {
            logWarn("repeat build!");
            return Conf.createConf(this.getConfig());
        }

        // Resolve substitutions.
        this.config = this.config.resolve();

        //过滤固定前缀变量
        if (this.usePrefix != null) {
            final Set<Map.Entry<String, ConfigValue>> entries = config.entrySet();
            final Map<String, ConfigValue> cp = new HashMap<>(entries.size());
            entries.forEach(e -> {
                final String key = e.getKey();
                if (key.startsWith(this.usePrefix)) {
                    cp.put(key, e.getValue());
                }
            });

            final Config configCp = ConfigFactory.parseMap(cp);
            logDebug("Logging properties. Make sure sensitive data such as passwords or secrets are not logged!");
            logDebug(configCp.root().render(CONFIG_RENDER_OPTIONS_JSON));
            this.config = configCp;
        }
        this.build = true;
        return Conf.createConf(this.getConfig());
    }
}
