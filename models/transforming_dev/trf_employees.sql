{{config(materialized='table', schema=env_var('DBT_TRANSFORMING_SCHEMA', 'TRANSFORMING_DEV'))}}


with recursive managers 
      -- Column names for the "view"/CTE
      (indent, empid, reportsto, firstname, lastname, managername, title, hiredate, office, extension, yearsalary) 
    as
      -- Common Table Expression
      (
        -- Anchor Clause
        select '' as indent, empid, reportsto, firstname, lastname, firstname as managername, title, hiredate, office, extension, yearsalary
          from {{ref("stg_employees")}}
          where title = 'President'

        union all

        -- Recursive Clause
        select indent || '* ',
            e.empid, e.reportsto, e.firstname, e.lastname, m.firstname as managername, e.title, e.hiredate, e.office, e.extension, e.yearsalary
          from {{ref("stg_employees")}} as e join managers as m
            on e.reportsto = m.empid
      )
  -- This is the "main select".
  select indent || title as title, empid, firstname, lastname, managername, hiredate, extension, yearsalary,
  officecity, officestateprovince as officestate, officecountry
    from managers emp
    left join {{ref("stg_offices")}} as off
    on off.office = emp.office
