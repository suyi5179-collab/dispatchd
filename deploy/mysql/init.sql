-- Tests create one throwaway database each, so the app user may create
-- databases. Local demo only.
GRANT ALL PRIVILEGES ON *.* TO 'dispatchd'@'%';
FLUSH PRIVILEGES;
