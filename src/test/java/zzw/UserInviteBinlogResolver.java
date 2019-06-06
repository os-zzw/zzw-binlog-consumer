package zzw;

import static org.apache.commons.collections4.MapUtils.getLong;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import zzw.model.BaseChangedRow;
import zzw.model.UpdatedRow;
import zzw.util.BinlogResolver;

/**
 * @author zhangzhewei
 */
public class UserInviteBinlogResolver implements BinlogResolver {

    private Set<String> tableNamePrefixes = ImmutableSet.of("user_invite");

    private static final Logger logger = LoggerFactory.getLogger(UserInviteBinlogResolver.class);

    @Override
    public void onUpdate(BaseChangedRow row) throws Exception {
        Map<String, Serializable> affectedRow = row.getAffectedRow();
        long id = getLong(affectedRow, "id");
        long userId = getLong(affectedRow, "user_id");
        long inviteUserId = getLong(affectedRow, "invite_user_id");
        logger.info("zzw:{}-{}-{}", id, userId, inviteUserId);
        switch (row.getType()) {
            case INSERT:
                break;
            case UPDATE:
                Map<String, Serializable> before = ((UpdatedRow) row).getBefore();
                long idBefore = getLong(before, "id");
                long userIdBefore = getLong(before, "user_id");
                long inviteUserIdBefore = getLong(before, "invite_user_id");
                logger.info("zzw:{}-{}-{}", idBefore, userIdBefore, inviteUserIdBefore);
                logger.info("zzw:update");
                break;
            case DELETE:
                logger.info("zzw:delete");
                break;
        }
    }

    @Override
    public boolean acceptTableName(String tableName) {
        return tableNamePrefixes.stream().anyMatch(tableName::startsWith);
    }

}
