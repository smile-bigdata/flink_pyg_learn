/* postgres  */
--第一题
DROP TABLE IF EXISTS student;

CREATE TABLE student (
  id numeric(20) primary key ,
  name varchar(50) ,
  score numeric
) ; 

--第二题
 create unique index stu_key on student(name);
alter table student alter column name type varchar(100);
create view stu_view as select * from student; 

--第三题
insert into student (id,name,score)
values(1001,'jack',89),
(1002,'Tom',92.5),
(1003,'smith',93.4),
(1004,'唐三',92.5),
(1234,'lucy',95); 

update student set name='小舞',score=92.4 where id =1234;
update student set score=58 where id=1001; 

--error
/* insert into student select id,name,score from student on 
conflict(1003) do update set name='jack',score=78; */

--trim ltrim rtrim 三个函数的使用
select trim(' flink ') as skills from student;
select rtrim(' apache ') as skills from student; 

-- select * from student ;

/* alter table stdent drop column if exists score_level;
alter table student add column score_level  varchar(20); */

select 
    t.id,t.name,
	case 
		when t.score >=90 then '优秀'
	    when t.score>=80 then '良好'
		when t.score>=60 then '及格'
		else 
		    '缺考'
		end as score_level
from student t; 

--第四题
create table student_bak as select * from student;


-- returns varchar declare also varchar
create or replace function score_grade(score numeric)
returns varchar(10) language plpgsql as 
$$
declare
score_grade varchar(10);
begin 
	 if score >=90 then score_grade:='优秀';
	 elseif score>=60 and score <90 then score_grade:='良好';
	 elseif score<60 and score >=0 then score_grade:='不及格';
	 else score_grade:='缺考';
	 end if;
	 return score_grade;
end;
$$

/* oracle数据库的语法 */
--第一题
drop table if exists student_score;
create table student_score(
	 id number primary key,
	 name varchar2(50),
	 score number
) tablespace student_data;

--第二题
create unique index stu_sco_key on student_score(name);
alter table student_score modify name varchar2(100);
create or replace view student_score_v as select * from student_score;

--第三题
insert into student_score(id,name,score)
values(1001,'jack',89),
(1002,'Tom',92.5),
(1003,'smith',93.4),
(1004,'唐三',92.5),
(1234,'lucy',95); 

alter table student_score add score_level varchar2(50);

select 
    t.id,t.name,t.score,
	case 
		when t.score >=85 then 'A'
	    when t.score>=60 and t.score<85 then 'B'
		when t.score>=0   and t.score<60 then 'C'
		else 
		    '缺考'
		end as score_level
from student_score t; 

--第四题
create table student_score_bak as select * from student_score;
drop table student_score;
rename student_score_bak to student_score;

create or replace function level(score number)
return number is scorelevel number,
begin
	 if score >= 85
		 then scorelevel:=3;
	 elsif score>=60 and score<80 
		 then scorelevel:=2;
	 elsif score <60 and score >=0
		 then scorelevel:=1;
	 end if;
	 return scorelevel;
end;










