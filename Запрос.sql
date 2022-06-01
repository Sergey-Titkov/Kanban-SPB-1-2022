-- tab=jira_worklog
-- Список всех эпиков
with qre_epic as
 (select jrie.*,
    from Jiraissue jrie
   where jrie.project = -- ID Проекта
     and jrie.issuetype = -- ID Типа эпик
  ),
-- Как работу работаем, списание идет как на таски так и на саб таски
qre_fired as
 (select trunc(wg.startdate) "Дата списания",
         "Эпик",
         "Команда",
         sum(wg.timeworked) / 8 / 3600 "Потрачено"
    from (select all_id, "Эпик", "Команда"
          
            from (
                  -- Получаем задачи --------------------------------------------------------------------------------------------------------------------------------------------------------
                  select jrie_ts.id all_id,
                          jrie_ep.summary "Эпик",
                          (select cfo.customvalue
                             from customfield       cf,
                                  customfieldvalue  cfv,
                                  customfieldoption cfo
                            where cf.cfname = 'Команда'
                              and cf.id = cfv.customfield
                              and TO_NUMBER(cfv.stringvalue DEFAULT -1 ON
                                                                    CONVERSION
                                                                    ERROR) =
                                  cfo.id
                              and cfv.issue = jrie_ts.id) "Команда"
                    from qre_epic qe_ec
                   inner join Jiraissue jrie_ep
                      on qe_ec.id = jrie_ep.id
                  -- Связь эпиков и задач
                   inner join Issuelink ielk_ep_ts
                      on ielk_ep_ts.source = qe_ec.id
                     and ielk_ep_ts.linktype = -- ID Линка на эпик
                   inner join Jiraissue jrie_ts
                      on jrie_ts.id = ielk_ep_ts.destination
                  
                   where exists (select *
                            from Label ll
                           where ll.issue = jrie_ts.id
                             and ll.label = -- Метка релиза)
                  
                  union
                  
                  -- Получаем под задачи --------------------------------------------------------------------------------------------------------------------------------------------------------
                  select jrie_st.id all_id,
                          jrie_ep.summary "Эпик",
                          (select cfo.customvalue
                             from customfield       cf,
                                  customfieldvalue  cfv,
                                  customfieldoption cfo
                            where cf.cfname = 'Команда'
                              and cf.id = cfv.customfield
                              and TO_NUMBER(cfv.stringvalue DEFAULT -1 ON
                                                                    CONVERSION
                                                                    ERROR) =
                                  cfo.id
                              and cfv.issue = jrie_ts.id) "Команда"
                    from qre_epic qe_ec
                   inner join Jiraissue jrie_ep
                      on qe_ec.id = jrie_ep.id
                  -- Связь эпиков и задач
                   inner join Issuelink ielk_ep_ts
                      on ielk_ep_ts.source = qe_ec.id
                     and ielk_ep_ts.linktype = -- ID Линка на эпик
                   inner join Jiraissue jrie_ts
                      on jrie_ts.id = ielk_ep_ts.destination
                  
                  -- Связь задач и подзадач
                   inner join Issuelink ielk_ts_st
                      on ielk_ts_st.source = jrie_ts.id
                     and ielk_ts_st.linktype = -- ID Линка на задачу
                   inner join Jiraissue jrie_st
                      on jrie_st.id = ielk_ts_st.destination
                   where (exists (select *
                            from Label ll
                           where ll.issue = jrie_ts.id
                             and ll.label = -- Метка релиза)
                  
                  )) all_task
   inner join Worklog wg
      on all_task.all_id = wg.issueid
   where trunc(wg.startdate) >= to_date( -- Дата начала реализации релиза, 'DD.MM.YYYY')
   group by trunc(wg.startdate), "Эпик", "Команда"
  
   order by trunc(wg.startdate)),

-- Добавленная работа, если таску создали после начала работы, значит это добавленная работа
qre_added as
 (select trunc(created) "Дата добавления",
         sum(timeoriginalestimate) / 8 / 3600 "Добавлено",
         "Эпик",
         "Команда"
    from (
          
          -- Получаем задачи --------------------------------------------------------------------------------------------------------------------------------------------------------
          select
          -- Задачи
           jrie_ts.created,
            jrie_ts.timeoriginalestimate,
            jrie_ep.summary "Эпик",
            (select cfo.customvalue
               from customfield       cf,
                    customfieldvalue  cfv,
                    customfieldoption cfo
              where cf.cfname = 'Команда'
                and cf.id = cfv.customfield
                and TO_NUMBER(cfv.stringvalue DEFAULT -1 ON CONVERSION ERROR) =
                    cfo.id
                and cfv.issue = jrie_ts.id) "Команда"
          
            from qre_epic qe_ec
           inner join Jiraissue jrie_ep
              on qe_ec.id = jrie_ep.id
          -- Связь эпиков и задач
           inner join Issuelink ielk_ep_ts
              on ielk_ep_ts.source = qe_ec.id
             and ielk_ep_ts.linktype = -- ID Линка на эпик
           inner join Jiraissue jrie_ts
              on jrie_ts.id = ielk_ep_ts.destination
          
           where (exists (select *
                            from Label ll
                           where ll.issue = jrie_ts.id
                             and ll.label = -- Метка релиза)
             and trunc(jrie_ts.created) >= to_date( -- Дата начала реализации релиза, 'DD.MM.YYYY')
          
          )
   group by trunc(created), "Эпик", "Команда"
   order by trunc(created)),
-- Календарь от даты работы релиза до  текущей даты + 90 дней   
fired_all as
 (select "Дата",
         "Эпик",
         "Команда",
         nvl("Потрачено", 0) "Потрачено"
    from (select to_date( -- Дата начала реализации релиза, 'DD.MM.YYYY') + level "Дата"
            from dual
          connect by level <=
                     trunc(sysdate) - to_date( -- Дата начала реализации релиза, 'DD.MM.YYYY') + 90) k
    left join qre_fired t
      on t."Дата списания" = k."Дата"),
-- Календарь от даты работы релиза до  текущей даты + 90 дней   
aded_all as
 (select "Дата",
         "Эпик",
         "Команда",
         nvl("Добавлено", 0) "Добавлено"
    from (select to_date( -- Дата начала реализации релиза, 'DD.MM.YYYY') + level "Дата"
            from dual
          connect by level <=
                     trunc(sysdate) - to_date( -- Дата начала реализации релиза, 'DD.MM.YYYY') + 90) k
  
    left join qre_added z
      on z."Дата добавления" = k."Дата")

select *
  from (select nvl(fired_all."Дата", aded_all."Дата") "Дата",
               nvl(fired_all."Эпик", aded_all."Эпик") "Эпик",
               nvl(fired_all."Команда", aded_all."Команда") "Команда",
               nvl(fired_all."Потрачено", 0) "Потрачено",
               nvl(aded_all."Добавлено", 0) "Добавлено"
        
          from fired_all
          full outer join aded_all
            on aded_all."Дата" = fired_all."Дата"
           and aded_all."Эпик" = fired_all."Эпик"
           and aded_all."Команда" = fired_all."Команда"
        
        union
        select to_date( -- Дата начала реализации релиза, 'DD.MM.YYYY') "Дата",
               "Эпик",
               "Команда",
               0 "Потрачено",
               sum(timeoriginalestimate) / 8 / 3600 "Добавлено"
          from (
                
                -- Получаем задачи --------------------------------------------------------------------------------------------------------------------------------------------------------
                select
                -- Задачи
                 jrie_ts.timeoriginalestimate,
                  jrie_ts.created,
                  jrie_ep.summary "Эпик",
                  (select cfo.customvalue
                     from customfield       cf,
                          customfieldvalue  cfv,
                          customfieldoption cfo
                    where cf.cfname = 'Команда'
                      and cf.id = cfv.customfield
                      and TO_NUMBER(cfv.stringvalue DEFAULT -1 ON CONVERSION
                                                            ERROR) = cfo.id
                      and cfv.issue = jrie_ts.id) "Команда"
                  from qre_epic qe_ec
                 inner join Jiraissue jrie_ep
                    on qe_ec.id = jrie_ep.id
                -- Связь эпиков и задач
                 inner join Issuelink ielk_ep_ts
                    on ielk_ep_ts.source = qe_ec.id
                   and ielk_ep_ts.linktype = -- ID Линка на эпик
                 inner join Jiraissue jrie_ts
                    on jrie_ts.id = ielk_ep_ts.destination
                
                 where (jrie_ts.timeoriginalestimate is not null and jrie_ts.timeoriginalestimate > 0 )
                   and (exists (select *
                            from Label ll
                           where ll.issue = jrie_ts.id
                             and ll.label = -- Метка релиза)
                   and trunc(jrie_ts.created) <=
                       to_date( -- Дата начала реализации релиза, 'DD.MM.YYYY')
                
                )
         group by "Эпик", "Команда")
 order by "Дата"
