for dir in *
do
        mv $dir/snapshots/snapshot_tag/* $dir/ 2>/dev/null
        rm -rf $dir/snapshots 2>/dev/null
        mv $dir/backups/* $dir/ 2>/dev/null
        rm -rf $dir/backups 2>/dev/null
        sstableloader -d 127.0.0.1 -u cassandra -pw cassandra ../keyspace_name/$dir
done