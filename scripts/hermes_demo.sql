insert into users(user_name, password, created_at)  values ('reviewer', '$2b$12$egUzuU2ajc422dCG6Qoow.6xVrm5bix/04hShnnwjYpmRgEDAUqeu', NOW());
insert into users(user_name, password, created_at)  values ('uploader1', '$2b$12$egUzuU2ajc422dCG6Qoow.6xVrm5bix/04hShnnwjYpmRgEDAUqeu', NOW());
insert into users(user_name, password, created_at)  values ('uploader2', '$2b$12$egUzuU2ajc422dCG6Qoow.6xVrm5bix/04hShnnwjYpmRgEDAUqeu', NOW());

insert into roles(role) values('admin');
insert into roles(role) values('reviewer');
insert into roles(role) values('uploader');

insert into permissions(permission) values ('approveUpload');
insert into permissions(permission) values ('createDataset');
insert into permissions(permission) values ('deleteDataset');
insert into permissions(permission) values ('analyzeDataset');

insert into permissions(permission) values ('approveUpload');
insert into permissions(permission) values ('createDataset');
insert into permissions(permission) values ('deleteDataset');
insert into permissions(permission) values ('analyzeDataset');

insert into role_permissions(permission_id, role_id) values (1, 1);
insert into role_permissions(permission_id, role_id) values (2, 2);
insert into role_permissions(permission_id, role_id) values (3, 2);
insert into role_permissions(permission_id, role_id) values (4, 3);
insert into role_permissions(permission_id, role_id) values (3, 3);

insert into user_roles(user_id, role_id) values (1, 1);
insert into user_roles(user_id, role_id) values (3, 3);
insert into user_roles(user_id, role_id) values (4, 2);

insert into groups(group) value ('hermes')
insert into user_groups(group_id, user_id) values (1, 2)
insert into user_groups(group_id, user_id) values (1, 3)
insert into user_groups(group_id, user_id) values (1, 4)
