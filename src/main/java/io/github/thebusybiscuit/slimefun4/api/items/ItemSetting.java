package io.github.thebusybiscuit.slimefun4.api.items;

import io.github.thebusybiscuit.cscorelib2.config.Config;
import io.github.thebusybiscuit.slimefun4.implementation.SlimefunPlugin;
import me.mrCookieSlime.Slimefun.Objects.SlimefunItem.SlimefunItem;
import org.apache.commons.lang.Validate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

/**
 * This class represents a Setting for a {@link SlimefunItem} that can be modified via
 * the {@code Items.yml} {@link Config} file.
 *
 * @param <T> The type of data stored under this {@link ItemSetting}
 * @author TheBusyBiscuit
 */
public class ItemSetting<T> {

    private SlimefunItem item;

    private final String key;
    private final T defaultValue;

    private T value;

    /**
     * This creates a new {@link ItemSetting} with the given key and default value
     *
     * @param item
     *            The {@link SlimefunItem} this {@link ItemSetting} belongs to
     * @param key
     *            The key under which this setting will be stored (relative to the {@link SlimefunItem})
     * @param defaultValue
     *            The default value for this {@link ItemSetting}
     */
    @ParametersAreNonnullByDefault
    public ItemSetting(SlimefunItem item, String key, T defaultValue) {
        Validate.notNull(key, "The key of an ItemSetting is not allowed to be null!");
        Validate.notNull(defaultValue, "The default value of an ItemSetting is not allowed to be null!");

        this.item = item;
        this.key = key;
        this.defaultValue = defaultValue;
    }

    /**
     * This creates a new {@link ItemSetting} with the given key and default value
     *
     * @deprecated Please use the other constructor.
     *
     * @param key
     *            The key under which this setting will be stored (relative to the {@link SlimefunItem})
     * @param defaultValue
     *            The default value for this {@link ItemSetting}
     */
    @Deprecated
    public ItemSetting(String key, T defaultValue) {
        this(null, key, defaultValue);
    }

    /**
     * This method checks if a given input would be valid as a value for this
     * {@link ItemSetting}. You can override this method to implement your own checks.
     *
     * @param input
     *            The input value to validate
     *
     * @return Whether the given input was valid
     */
    public boolean validateInput(T input) {
        return input != null;
    }

    /**
     * This method updates this {@link ItemSetting} with the given value.
     * Override this method to catch changes of a value.
     * A value may never be null.
     *
     * @param newValue
     *            The new value for this {@link ItemSetting}
     */
    public void update(@Nonnull T newValue) {
        if (validateInput(newValue)) {
            this.value = newValue;
        } else {
            throw new IllegalArgumentException("The passed value was not valid. (Maybe null?)");
        }

        // Feel free to override this as necessary.
    }

    /**
     * This returns the key of this {@link ItemSetting}.
     *
     * @return The key under which this setting is stored (relative to the {@link SlimefunItem})
     */
    @Nonnull
    public String getKey() {
        return key;
    }

    /**
     * This returns the <strong>current</strong> value of this {@link ItemSetting}.
     *
     * @return The current value
     */
    @Nonnull
    public T getValue() {
        if (value != null) {
            return value;
        } else if (item != null) {
            item.warn("ItemSetting '" + key + "' was invoked but was not initialized yet.");
            return defaultValue;
        } else {
            throw new IllegalStateException("ItemSetting '" + key + "' was invoked but was not initialized yet.");
        }
    }

    /**
     * This returns the <strong>default</strong> value of this {@link ItemSetting}.
     *
     * @return The default value
     */
    @Nonnull
    public T getDefaultValue() {
        return defaultValue;
    }

    /**
     * This method checks if this {@link ItemSetting} stores the given data type.
     *
     * @param c
     *            The class of data type you want to compare
     * @return Whether this {@link ItemSetting} stores the given type
     */
    public boolean isType(@Nonnull Class<?> c) {
        return c.isInstance(defaultValue);
    }

    /**
     * This is an error message which should provide further context on what values
     * are allowed.
     *
     * @return An error message which is displayed when this {@link ItemSetting} is misconfigured.
     */
    @Nonnull
    protected String getErrorMessage() {
        return "请使用在 '" + defaultValue.getClass().getSimpleName() + "' 范围内的值!";
    }

    /**
     * This method is called by a {@link SlimefunItem} which wants to load its {@link ItemSetting}
     * from the {@link Config} file.
     */
    @SuppressWarnings("unchecked")
    public void reload() {
        Validate.notNull(item, "Cannot apply settings for a non-existing SlimefunItem");

        SlimefunPlugin.getItemCfg().setDefaultValue(item.getId() + '.' + getKey(), getDefaultValue());
        Object configuredValue = SlimefunPlugin.getItemCfg().getValue(item.getId() + '.' + getKey());

        if (defaultValue.getClass().isInstance(configuredValue)) {
            // We can do an unsafe cast here, we did an isInstance(...) check before!
            T newValue = (T) configuredValue;

            if (validateInput(newValue)) {
                this.value = newValue;
            } else {
                // @formatter:off
                item.warn(
                        "发现在 Items.yml 中有无效的物品设置!" +
                                "\n  在 \"" + item.getId() + "." + getKey() + "\"" +
                                "\n  " + configuredValue + " 不是一个有效值!" +
                                "\n" + getErrorMessage()
                );
                // @formatter:on
            }
        } else {
            this.value = defaultValue;
            String found = configuredValue == null ? "null" : configuredValue.getClass().getSimpleName();

            // @formatter:off
            item.warn(
                    "发现在 Items.yml 中有无效的物品设置!" +
                            "\n请只设置有效的值." +
                            "\n  在 \"" + item.getId() + "." + getKey() + "\"" +
                            "\n  期望值为 \"" + defaultValue.getClass().getSimpleName() + "\" 但填写了: \"" + found + "\""
            );
            // @formatter:on

        }
    }

    @Override
    public String toString() {
        T currentValue = this.value != null ? this.value : defaultValue;
        return getClass().getSimpleName() + " {" + getKey() + " = " + currentValue + " (default: " + getDefaultValue() + ")";
    }

    /**
     * This is a temporary workaround and will be removed.
     *
     * @deprecated Do not use this method!
     * @param item
     *            the item
     */
    @Deprecated
    public void reload(@Nullable SlimefunItem item) {
        this.item = item;
        reload();
    }

}