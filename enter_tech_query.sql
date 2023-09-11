SELECT 
    u.id as id_user,
    u.city as ds_user_city,
    u.institution as ds_institution,
    u.firstaccess as ts_firstaccess,
    u.lastaccess as ts_lastaccess,
    datediff(current_date(), from_unixtime(u.firstaccess)) as n_days_since_firstaccess,
    datediff(current_date(), from_unixtime(u.lastaccess)) as n_days_since_lastaccess,
    (SELECT COUNT(1) FROM mdl_assign_user_flags auf WHERE auf.userid = u.id) as n_assignments_flagged,
    (SELECT COUNT(1) FROM mdl_assign_user_mapping aum WHERE aum.userid = u.id) as n_assignments_mapped,
    (SELECT COUNT(1) FROM mdl_forum_discussions mfd WHERE mfd.userid = u.id) as n_forum_discussions_posted,
    (SELECT COUNT(1) FROM mdl_quiz_attempts mqa WHERE mqa.userid = u.id AND state = 'abandoned') as n_quiz_abandoned,
    (SELECT COUNT(1) FROM mdl_quiz_attempts mqa WHERE mqa.userid = u.id AND state = 'finished') as n_quiz_finished,
    (SELECT AVG(timefinish-timestart) from mdl_quiz_attempts mqa where mqa.timefinish > 0 and mqa.timestart > 0 and mqa.userid = u.id) as vl_avg_quiz_speed,
    (SELECT AVG(timefinish-timestart) from mdl_quiz_attempts mqa where mqa.timefinish > 0 and mqa.timestart > 0 and mqa.sumgrades > 0 and mqa.userid = u.id) as vl_avg_graded_quiz_speed,
    (SELECT SUM(qa.sumgrades)*100 / sum(q.sumgrades) from mdl_quiz_attempts qa join mdl_quiz q on q.id = qa.quiz where qa.userid = u.id group by userid having count(distinct qa.quiz) > 3) as vl_quiz_performance,
    (SELECT datediff(max(from_unixtime(timecreated)), min(from_unixtime(timecreated))) from mdl_logstore_standard_log lsl where lsl.userid = u.id) as n_interval_using,
    (SELECT COUNT( DISTINCT from_unixtime(timecreated, '%Y%m%d') ) from mdl_logstore_standard_log lsl where lsl.userid = u.id) as n_days_using,
    (SELECT COUNT(1) / datediff(date_add(max(from_unixtime(lsl.timecreated)), INTERVAL 1 DAY), min(from_unixtime(lsl.timecreated))) from mdl_logstore_standard_log lsl where lsl.timecreated is not null and lsl.userid = u.id) as vl_avg_daily_activity,
    (SELECT (COUNT(distinct from_unixtime(lsl.timecreated, '%Y%m%d')) * 100) / datediff(date_add(max(from_unixtime(lsl.timecreated)), INTERVAL 1 DAY), min(from_unixtime(timecreated))) as user_engagement from mdl_logstore_standard_log lsl where lsl.timecreated is not null and lsl.userid = u.id having count(distinct from_unixtime(lsl.timecreated, '%Y%m%d')) > 10) as vl_user_engagement_by_interval,
    (SELECT SUM(1) / COUNT(distinct from_unixtime(lsl.timecreated, '%Y%m%d')) from mdl_logstore_standard_log lsl where lsl.timecreated is not null and lsl.userid = u.id having count(distinct from_unixtime(lsl.timecreated, '%Y%m%d')) > 10) as vl_user_engagement_intra_day,
    (SELECT count(distinct from_unixtime(lsl.timecreated, '%Y%m%d')) from mdl_logstore_standard_log lsl where lsl.userid = u.id) as n_user_interactions,
    (SELECT count(distinct from_unixtime(lsl.timecreated, '%Y%m%d')) from mdl_logstore_standard_log lsl where LOWER(lsl.eventname) like '%mod_forum%' AND lsl.userid = u.id) as n_days_discussion_engagement,
    (SELECT count(distinct from_unixtime(lsl.timecreated, '%Y%m%d')) from mdl_logstore_standard_log lsl where lsl.eventname like '%mod_quiz%' AND lsl.userid = u.id) as n_days_quiz_engagement,
    (SELECT count(1) from mdl_logstore_standard_log lsl where lsl.eventname like '%mod_forum%' and lsl.userid = u.id) as n_discussion_engagement,
    (SELECT count(1) from mdl_logstore_standard_log lsl where eventname like '%mod_quiz%' and lsl.userid = u.id) as n_quiz_engagement,
    (SELECT count(1) from mdl_question_attempt_steps qas where qas.state like 'complete' and qas.userid = u.id) as n_questions_answered,
    (SELECT count(1) from mdl_question_attempt_steps qas where qas.state like 'gradedright' and qas.userid = u.id) as n_questions_rigth,
    (SELECT count(1) from mdl_question_attempt_steps qas where qas.state like 'gradedwrong' and qas.userid = u.id) as n_questions_wrong,
    (SELECT count(1) from mdl_question_attempt_steps qas where qas.state like 'gradedpartial' and qas.userid = u.id) as n_questions_partial,
    (SELECT SUM(gg.rawgrade) / SUM(gg.rawgrademax) from mdl_grade_grades gg where gg.userid = u.id and gg.aggregationstatus = 'used') as vl_user_performance,
    (SELECT COUNT(1) / datediff( from_unixtime(max(gg.timemodified)), from_unixtime(min(gg.timemodified)) ) from mdl_grade_grades gg where gg.userid = u.id and gg.aggregationstatus = 'used') as vl_avg_questions_by_day,
    (SELECT COUNT(distinct from_unixtime(gg.timemodified, '%Y%m%d')) / datediff( from_unixtime(max(gg.timemodified)), from_unixtime(min(gg.timemodified)) ) from mdl_grade_grades gg where gg.userid = u.id and gg.aggregationstatus = 'used') as vl_grades_engagement,
    (SELECT COUNT(1) from mdl_grade_grades gg where gg.userid = u.id and gg.aggregationstatus = 'used') as n_items_graded,
    (SELECT COUNT(1) from mdl_grade_grades gg where userid = u.id and gg.aggregationstatus != 'used') as n_items_not_responded,
    (SELECT AVG(gg.rawgrade / gg.rawgrademax) from mdl_grade_grades gg where gg.userid = u.id and gg.aggregationstatus = 'used') as vl_grade_average
FROM 
    mdl_user u 
    JOIN mdl_role_assignments ra ON ra.userid = u.id
    JOIN mdl_role r ON r.id = ra.roleid
    JOIN mdl_user_enrolments mue ON mue.userid = u.id
    JOIN mdl_enrol e ON e.id = mue.enrolid 
WHERE 
    u.confirmed = 1
    AND u.deleted = 0
    AND u.suspended = 0
    AND LOWER(r.name) = 'enternauta'
    AND e.courseid IN (832);