create or replace package staff_proc as
    function get_fio(p_pass_no number default 100) return varchar2;
    procedure get_border_salary(p_max_salary out number default 0, p_min_salary out number default 0);
    function update_empl(p_staff_rec test_staff%rowtype) return number;
end;

create or replace package body staff_proc as
function get_fio(p_pass_no number default 100) return varchar2 is
    v_fio varchar2(81);
begin
    begin
        select 
            first_name||' '||last_name
        into
            v_fio
        from
            test_staff
        where 
            pass_no = p_pass_no;
        return v_fio;
    exception
    when no_data_found then
        return '-';
    when others then
        raise;
    end;
end;

procedure get_border_salary(p_max_salary out number default 0, p_min_salary out number default 0) is
    v_cur_max number;
    v_cur_min number;
begin
    for v_iter in (select salary from test_staff) loop
        if v_cur_max is null or v_iter.salary > v_cur_max then
            v_cur_max := v_iter.salary;
        end if;
        if v_cur_min is null or v_iter.salary < v_cur_min then
            v_cur_min := v_iter.salary;
        end if;
    end loop;
    if v_cur_max is not null then
        p_max_salary := v_cur_max;
        p_min_salary := v_cur_min;
    end if;
end;

function update_empl(p_staff_rec test_staff%rowtype) return number is
    v_pass_no number;
begin
    begin
        select
            pass_no
        into
            v_pass_no
        from
            test_staff
        where
            pass_no = p_staff_rec.pass_no;
        execute immediate 'update test_staff set salary = '||p_staff_rec.salary||' where pass_no = '||v_pass_no;
        return -1;
    exception
    when no_data_found then
        execute immediate 'insert into test_staff values('||p_staff_rec.id||', '||p_staff_rec.first_name||', '||p_staff_rec.last_name||', '||p_staff_rec.pass_no||', '||p_staff_rec.salary||')';
        return p_staff_rec.pass_no;
    when others then
        raise;
    end;
end;
end;