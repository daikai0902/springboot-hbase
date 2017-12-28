package com.daikai.hbase.service.impl;

import com.daikai.hbase.service.DataService;
import com.daikai.hbase.vo.ResultVO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import javax.sql.RowSet;
import java.util.List;
import java.util.Map;

/**
 * @autor kevin.dai
 * @Date 2017/12/27
 */

@Service
public class DataServiceImpl  implements DataService{

    @Autowired
    @Qualifier("phoenixJdbcTemplate")
    JdbcTemplate  jdbcTemplate;

    public ResultVO add() {
        return jdbcTemplate.update("upsert into ORG_DEPT_NC (id,NAME ) VALUES ('8888888888888','dktest')") == 1?
                new ResultVO(true,"插入成功！"):new ResultVO(false,"插入失败！");
    }

    public int countDept() {
        return jdbcTemplate.queryForObject("select count(1) from ORG_DEPT_NC ",Integer.class);
    }

    public ResultVO delete() {
       return jdbcTemplate.update("delete from ORG_DEPT_NC WHERE ID = '8888888888888' ") == 1?
                new ResultVO(true,"删除成功！"):new ResultVO(false,"删除失败！");
    }

    public ResultVO update() {
        return jdbcTemplate.update("upsert into ORG_DEPT_NC (id,NAME ) VALUES ('0001N110000000VHA5QR','北区共享组01')") == 1?
                new ResultVO(true,"更新成功！"):new ResultVO(false,"更新失败！");
    }

    public List<Map<String, Object>> query() {
        return jdbcTemplate.queryForList("select * from ORG_DEPT_NC limit 3 ");
    }
}
