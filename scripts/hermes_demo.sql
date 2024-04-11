--password used is password
insert into users(user_name, password, created_at)  values ('reviewer', '$2b$12$egUzuU2ajc422dCG6Qoow.6xVrm5bix/04hShnnwjYpmRgEDAUqeu', NOW());
set @reviewer_user_id = LAST_INSERT_ID();
insert into users(user_name, password, created_at)  values ('uploader1', '$2b$12$egUzuU2ajc422dCG6Qoow.6xVrm5bix/04hShnnwjYpmRgEDAUqeu', NOW());
set @uploader1_id = LAST_INSERT_ID();
insert into users(user_name, password, created_at)  values ('uploader2', '$2b$12$egUzuU2ajc422dCG6Qoow.6xVrm5bix/04hShnnwjYpmRgEDAUqeu', NOW());
set @uploader2_id = LAST_INSERT_ID();

INSERT INTO roles(role) VALUES('reviewer');
SET @reviewer_role_id = LAST_INSERT_ID();

INSERT INTO roles(role) VALUES('uploader');
SET @uploader_role_id = LAST_INSERT_ID();


insert into permissions(permission) values ('approveUpload');
set @approve_upload_id = LAST_INSERT_ID();

insert into role_permissions(permission_id, role_id) values (@reviewer_user_id, @reviewer_role_id);

insert into user_roles(user_id, role_id) values (@uploader1_id, @uploader_role_id);
insert into user_roles(user_id, role_id) values (@uploader2_id, @uploader_role_id);
insert into user_roles(user_id, role_id) values (@reviewer_user_id, @reviewer_role_id);

insert into groups(group_name) value ('hermes');
SET @hermes_group_id = LAST_INSERT_ID();
insert into user_groups(group_id, user_id) values (@hermes_group_id, @reviewer_user_id);
insert into user_groups(group_id, user_id) values (@hermes_group_id, @uploader1_id);
insert into user_groups(group_id, user_id) values (@hermes_group_id, @uploader2_id);
