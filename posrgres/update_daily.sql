CREATE TABLE users (   --- создание таблиц
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

create or replace function log_user_update() --- функция проверки на изменения и добавления в историческую таблицу
returns trigger as $$
begin
    if old.name is distinct from new.name then
        insert into users_audit(user_id, changed_by, field_changed, old_value, new_value)
        values (old.id, current_user, 'name', old.name, new.name);
    end if;

    if old.email is distinct from new.email then
        insert into users_audit(user_id,changed_by,field_changed,old_value,new_value)
        values (old.id,current_user,'email',old.email,new.email);
    end if;
    
    if old.role is distinct from new.role then
        insert into users_audit(user_id,changed_by,field_changed,old_value,new_value)
        values (old.id,current_user,'role',old.role,new.role);
    end if;

    return new;
end;
$$ language plpgsql;

create trigger trigger_log_user_update     --- тригер к функции выше
before update on users
for each row
execute function log_user_update();

insert into users (name,email,role)   --- наливаем данные для теста
values ('Sasha Perepechaev','alex@yandex.ru','DE'),
('Anvar','anvar@yandex.ru','DE'),
('Serega','serega@yandex.ru','AL')

update users
set role = 'DE'
where id = 6

update users
set email = 'perepechaev.alex@yandex.ru',
    name = 'Aleksandr Perepechaev'
where id = 4

CREATE EXTENSION IF NOT exists pg_cron  --- установка крона

create  or replace function users_audit_export()  --- функция создания файла и копирования в дерикторию
returns void as $$
declare
    v_filename text;
    v_sql text;
begin
    v_filename := '/tmp/users_audit_export_' || to_char(current_date,'yyyy-mm-dd') ||  '.csv';

    v_sql := format(
    'copy (select * from users_audit where changed_at::date = current_date) to %l with (format csv, header)',
    v_filename
);

    execute v_sql;

end;
$$ language plpgsql;

select cron.schedule(  --- установка крона на опред. время
    'daily_users_audit',
    '0 3 * * *',
    'select users_audit_export();'
);





