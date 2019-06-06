package zzw.util;

import static com.github.phantomthief.tuple.Tuple.tuple;
import static java.util.Collections.singletonMap;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;

import com.github.phantomthief.tuple.TwoTuple;

/**
 * @author zhangzhewei
 */
public class DDLUtils {

    private static final Supplier<List<Pattern>> DDL_PATTERNS = () -> {
        // (?:(?:(\w+)|`(\w+)`)\.)?(?:(\w+)|`(\w+)`)
        Map<String, String> name = singletonMap("pair",
                "(?:(?:(\\w+)|`(\\w+)`)\\.)?(?:(\\w+)|`(\\w+)`)");
        String[] patterns = { // 写正则什么的……
                "^create\\s+table\\s+${pair}", //
                "^alter\\s+table\\s+${pair}", //
                "^drop\\s+table\\s+(?:if\\s+exists\\s+)?${pair}", //
                "^rename\\s+table\\s+(?:${pair}\\s+to\\s+${pair}\\s*,\\s*)?${pair}\\s+to\\s+${pair}", // 只支持两次rename
        };
        return Stream.of(patterns) //
                .map(rawPattern -> StrSubstitutor.replace(rawPattern, name)) //
                .map(pattern -> compile(pattern, CASE_INSENSITIVE)) //
                .collect(toList());
    };

    /**
     * 从SQL(DDL)中解析出表名
     * 目前支持create/alter/rename/drop四种简单语法，通过正则匹配出
     * 我觉得没必要为这么个解析需求引入一个parser……
     *
     * @return first(库名)可为空，second(表名)不为空
     *
     * 请使用 {@link DDLUtils resolveTableNameFromDDL2(String)}
     */
    @Deprecated
    public static List<TwoTuple<String, String>> resolveTableNameFromDDL(String sql) {
        if (StringUtils.isEmpty(sql)) {
            return null;
        }
        return DDL_PATTERNS.get().stream() //
                .map(pattern -> pattern.matcher(sql)) //
                .map(matcher -> {
                    List<TwoTuple<String, String>> result = null;
                    while (matcher.find()) {
                        if (result == null) {
                            result = new ArrayList<>();
                        }
                        for (int i = 1; i < matcher.groupCount(); i += 4) {
                            String db = matcher.group(i);
                            if (db == null) {
                                db = matcher.group(i + 1);
                            }
                            String table = matcher.group(i + 2);
                            if (table == null) {
                                table = matcher.group(i + 3);
                            }
                            if (table != null) {
                                result.add(tuple(db, table));
                            }
                        }
                    }
                    return result;
                }) //
                .filter(Objects::nonNull) //
                .findFirst() //
                .orElse(null);
    }

    private static TwoTuple<String, String> getDbTablePair(String name) {
        name = StringUtils.remove(name, '`');
        int idx = name.indexOf('.');
        if (idx < 0) {
            return tuple(null, name);
        } else {
            return tuple(StringUtils.substringBefore(name, "."),
                    StringUtils.substringAfter(name, "."));
        }
    }
}
