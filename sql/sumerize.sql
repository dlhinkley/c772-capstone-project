-- Set empty dates to null
UPDATE sample_view_cleaner
SET was_fully_scored_datetime = (
    CASE
        WHEN DATE(was_fully_scored_datetime) = '2999-01-01' THEN NULL
        ELSE was_fully_scored_datetime
    END
    );
--
-- SELECT
--        assessment_item_response_id, -- assessment item instance delivered at launch
--        assessment_item_instance_id, -- delivered assessment independent of the item attempt but still for a specific student taking a specific test.
--        assessment_item_instance_attempt_id, -- specific delivered attempt of that item to a student taking a specific test instance.
--        assessment_item_deliver_id, -- assessment item instance delivered at launch
--        assessment_item_id, -- item as found in the authoring system
--        assessment_instance_attempt_id, -- assessment attempt taken by an individual student. This will not be present when the event is sent on_assign.
--        assessment_instance_id, -- assessment instance of at the class level same as assessment_instance_attempt - version/attempt number and student specific identifier.
--        assessment_id, --  authored assessment that correlates to this assessment instance.
--        learner_assigned_item_attempt_id,
--        learner_assignment_attempt_id, -- learner assignmen
--        learner_id, -- a student
--        section_id,
-- org_id,
-- assignment_number_of_items_served,
-- number_of_learners,
-- format_type_code,
-- is_affecting_grade,
-- raw_score,
--        scale_score,
--        is_scorable
-- FROM sample_view_cleaner
-- order by assessment_item_id, -- unique to assessement
--          assessment_instance_attempt_id, -- unique to learner attempt. Actually attempted
--          assessment_instance_id, -- unique to learner attempt. Actually attempted
--          assessment_id, -- unique to assessement
--          learner_assigned_item_attempt_id, -- unique to learner attempt. Assigned, but not attempt. the item of a learner attempt
--          learner_assignment_attempt_id, -- unique to learner attempt. Assigned, but not attempt. the learner attempt of an assignment
--          learner_id,
--          section_id,
--          org_id;

WITH answers_by_attempt AS (
    SELECT a.learner_assignment_attempt_id,
           count(a.assessment_item_response_id)::DECIMAL   num_questions_answered
    FROM (
        SELECT DISTINCT learner_assignment_attempt_id, learner_assigned_item_attempt_id, assessment_item_response_id
        FROM sample_view_cleaner
        WHERE learner_attempt_status = 'fully scored'
    ) a
    GROUP BY a.learner_assignment_attempt_id
),
     scores AS (
         SELECT DISTINCT (cl.learner_assignment_attempt_id)  AS attempt_id,
                         cl.assessment_id,
                         cl.learner_id,
                         cl.section_id,
                         cl.org_id,
                         cl.final_score_unweighted AS num_final_score,
                         cl.points_possible_unweighted AS num_possible_score,
--                          ROUND(cl.final_score_unweighted / cl.points_possible_unweighted * 100) AS pct_final_score,
                         was_fully_scored_datetime::date AS scored_date,
--                          ROUND( aba.num_questions_answered / cl.number_of_distinct_instance_items * 100) AS pct_questions_answered, -- the percentage of questions answered
                         cl.number_of_distinct_instance_items AS num_questions, -- number of questions
                         aba.num_questions_answered
         FROM sample_view_cleaner cl
         LEFT JOIN answers_by_attempt aba ON cl.learner_assignment_attempt_id = aba.learner_assignment_attempt_id
         WHERE learner_attempt_status = 'fully scored'
     ),
     learners AS (
         SELECT learner_id,
                section_id,
                org_id,
                MIN(was_fully_scored_datetime::date) AS min_scored_date,
                MAX(was_fully_scored_datetime::date) AS max_scored_date,
                MAX(was_fully_scored_datetime::date) - MIN(was_fully_scored_datetime::date) AS days
         FROM sample_view_cleaner
         GROUP BY learner_id, section_id, org_id
     ),
     sections AS (
         SELECT section_id,
                MIN(was_fully_scored_datetime::date) AS min_scored_date,
                MAX(was_fully_scored_datetime::date) AS max_scored_date,
                MAX(was_fully_scored_datetime::date) - MIN(was_fully_scored_datetime::date) AS days
         FROM sample_view_cleaner
         GROUP BY section_id
     ),
     assessments AS (
         SELECT assessment_id,
                MIN(was_fully_scored_datetime::date) AS min_scored_date,
                MAX(was_fully_scored_datetime::date) AS max_scored_date,
                MAX(was_fully_scored_datetime::date) - MIN(was_fully_scored_datetime::date) AS days
         FROM sample_view_cleaner
         GROUP BY assessment_id
     ),
     orgs AS (
         SELECT org_id,
                MIN(was_fully_scored_datetime::date) AS min_scored_date,
                MAX(was_fully_scored_datetime::date) AS max_scored_date,
                MAX(was_fully_scored_datetime::date) - MIN(was_fully_scored_datetime::date) AS days
         FROM sample_view_cleaner
         GROUP BY org_id
     ),
     score_by_learner AS ( -- How learners performed on all assessments attempts
         SELECT l.learner_id, --1126
                ROUND(AVG(s.num_final_score)) AS learner_num_final_score,
                ROUND(AVG(s.num_possible_score)) AS learner_num_possible_score,
--                 ROUND(AVG(s.pct_final_score)) AS learner_pct_final_score,
--                 ROUND(AVG(s.pct_questions_answered)) AS learner_pct_questions_answered,
                ROUND(AVG(s.num_questions_answered)) AS learner_num_questions_answered,
                ROUND(AVG(s.num_questions)) AS learner_num_questions,
                COUNT(*) AS learner_num_attempts,
                ROUND(AVG(l.days)) AS learner_days, -- The number of days the learner took assessments
                ROUND(AVG(l.days/4 * 1)) AS learner_t1_scored_days--, -- t1 ends the nth day of school year
--                 COALESCE(ROUND(AVG(CASE WHEN (s.scored_date - l.min_scored_date) BETWEEN 0 AND (l.days/4 * 1) THEN s.pct_final_score END)), 0) AS learner_t1_score, -- average score for attempts in t1
--                 COUNT(CASE WHEN (s.scored_date - l.min_scored_date) BETWEEN 0 AND (l.days/4 * 1) THEN 1 END) AS learner_t1_attempts, -- number of attempts for t1
--                 ROUND(AVG(l.days/4 * 2)) AS learner_t2_scored_days,
--                 COALESCE(ROUND(AVG(CASE WHEN (s.scored_date - l.min_scored_date) BETWEEN (l.days/4 * 1) + 1 AND (l.days/4 * 2) THEN s.pct_final_score END)), 0) AS learner_t2_score,
--                 COUNT(CASE WHEN (s.scored_date - l.min_scored_date) BETWEEN (l.days/4 * 1) + 1 AND (l.days/4 * 2) THEN 1 END) AS learner_t2_attempts,
--                 ROUND(AVG(l.days/4 * 3)) AS learner_t3_scored_days,
--                 COALESCE(ROUND(AVG(CASE WHEN (s.scored_date - l.min_scored_date) BETWEEN (l.days/4 * 2) + 1 AND (l.days/4 * 3) THEN s.pct_final_score END)), 0) AS learner_t3_score,
--                 COUNT(CASE WHEN (s.scored_date - l.min_scored_date) BETWEEN (l.days/4 * 2) + 1 AND (l.days/4 * 3) THEN 1 END) AS learner_t3_attempts,
--                 ROUND(AVG(l.days/4 * 4)) AS learner_t4_scored_days,
--                 COALESCE(ROUND(AVG(CASE WHEN (s.scored_date - l.min_scored_date) BETWEEN (l.days/4 * 3) + 1 AND (l.days/4 * 4) THEN s.pct_final_score END)), 0) AS learner_t4_score,
--                 COUNT(CASE WHEN (s.scored_date - l.min_scored_date) BETWEEN (l.days/4 * 3) + 1 AND (l.days/4 * 4) THEN 1 END) AS learner_t4_attempts
         FROM learners l,
              scores s
         WHERE l.learner_id = s.learner_id
         GROUP BY l.learner_id
     ),
     score_by_assessment  AS ( -- How all learners performed on attempts of an assessment
         SELECT a.assessment_id, -- 329
                ROUND(AVG(s.num_final_score)) AS assessment_num_final_score,
                ROUND(AVG(s.num_possible_score)) AS assessment_num_possible_score,
--                 ROUND(AVG(s.pct_final_score)) AS assessment_pct_final_score,
--                 ROUND(AVG(s.pct_questions_answered)) AS assessment_pct_questions_answered,
                ROUND(AVG(s.num_questions_answered)) AS assessment_num_questions_answered,
                ROUND(AVG(s.num_questions)) AS assessment_num_questions,
                COUNT(*) AS assessment_num_attempts
         FROM assessments a,
              scores s
         WHERE a.assessment_id = s.assessment_id
         GROUP BY a.assessment_id
     ),
     score_by_section  AS ( -- How all learners performed on attempts of an assessment by section
         SELECT a.section_id, --490
                s.assessment_id,
                ROUND(AVG(s.num_final_score)) AS section_num_final_score,
                ROUND(AVG(s.num_possible_score)) AS section_num_possible_score,
--                 ROUND(AVG(s.pct_final_score)) AS section_pct_final_score,
--                 ROUND(AVG(s.pct_questions_answered)) AS section_pct_questions_answered,
                ROUND(AVG(s.num_questions_answered)) AS section_num_questions_answered,
                ROUND(AVG(s.num_questions)) AS section_num_questions,
                COUNT(*) AS section_num_attempts,
                ROUND(AVG(a.days)) AS section_days
         FROM sections a,
              scores s
         WHERE a.section_id = s.section_id
         GROUP BY a.section_id, s.assessment_id
     ),
     score_by_org  AS (
         SELECT a.org_id, --329
                s.assessment_id,
                ROUND(AVG(s.num_final_score)) AS organization_num_final_score,
                ROUND(AVG(s.num_possible_score)) AS organization_num_possible_score,
--                 ROUND(AVG(s.pct_final_score)) AS organization_pct_final_score,
--                 ROUND(AVG(s.pct_questions_answered)) AS organization_pct_questions_answered,
                ROUND(AVG(s.num_questions_answered)) AS organization_num_questions_answered,
                ROUND(AVG(s.num_questions)) AS organization_num_questions,
                COUNT(*) AS organization_num_attempts,
                ROUND(AVG(a.days)) AS organization_days
         FROM orgs a,
              scores s
         WHERE a.org_id = s.org_id
         GROUP BY a.org_id, s.assessment_id
     )
-- SELECT
--     (select COUNT(distinct learner_assignment_attempt_id) FROM scores) as qty_scores,
--     (select COUNT(*) FROM score_by_learner) as qty_lrg,
--     (select COUNT(distinct assessment_id) FROM score_by_assessment) as qty_assess,
--     (select COUNT(*) FROM score_by_section) as qty_sec,
--     (select COUNT(*) FROM score_by_org) as qty_org;
-- +----------+-------+----------+-------+-------+
-- |qty_scores|qty_lrg|qty_assess|qty_sec|qty_org|
-- +----------+-------+----------+-------+-------+
-- |8855      |1126   |330       |493    |330    |
-- +----------+-------+----------+-------+-------+
-- SELECT md5(lrn.learner_id||params.secret) AS learner_id, learner_num_final_score, learner_num_possible_score, learner_pct_final_score, learner_pct_questions_answered, learner_num_questions_answered, learner_num_questions, learner_num_attempts, learner_days, learner_t1_scored_days, learner_t1_score, learner_t1_attempts, learner_t2_scored_days, learner_t2_score, learner_t2_attempts, learner_t3_scored_days, learner_t3_score, learner_t3_attempts, learner_t4_scored_days, learner_t4_score, learner_t4_attempts,
--        md5(assess.assessment_id||params.secret) AS assessment_id, assessment_num_final_score, assessment_num_possible_score, assessment_pct_final_score, assessment_pct_questions_answered, assessment_num_questions_answered, assessment_num_questions, assessment_num_attempts,
--        md5(sec.section_id||params.secret) AS section_id, section_num_final_score, section_num_possible_score, section_pct_final_score, section_pct_questions_answered, section_num_questions_answered, section_num_questions, section_num_attempts, section_days,
--        md5(org.org_id||params.secret) AS org_id, organization_num_final_score, organization_num_possible_score, organization_pct_final_score, organization_pct_questions_answered, organization_num_questions_answered, organization_num_questions, organization_num_attempts, organization_days
-- FROM scores
--     JOIN params ON TRUE
--     JOIN score_by_learner lrn ON lrn.learner_id = scores.learner_id -- 8855
--     JOIN score_by_assessment assess ON assess.assessment_id = scores.assessment_id -- 8892
--     JOIN score_by_section sec ON sec.section_id = scores.section_id AND sec.assessment_id = scores.assessment_id -- 8960
--     JOIN score_by_org org ON org.org_id = scores.org_id AND org.assessment_id = scores.assessment_id -- 9102
-- order by scores.org_id, scores.section_id, scores.assessment_id, scores.learner_id;
SELECT attempt_id,
       assessment_id,
       learner_id,
       section_id,
       org_id,
       scored_date,
       num_final_score,
       num_possible_score,
       num_questions,
       num_questions_answered
FROM scores s;
