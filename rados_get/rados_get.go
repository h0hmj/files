package main

import (
    "fmt"
    "io/ioutil"
    "os"
    "github.com/ceph/go-ceph/rados"
)

func main() {
    if len(os.Args) < 4 {
        fmt.Printf("Usage: %s <poolname> <objname> <output_file>\n", os.Args[0])
        return
    }

    poolname := os.Args[1]
    objname := os.Args[2]
    file_path := os.Args[3]

    cluster, err := rados.NewConn()
    if err != nil {
        fmt.Printf("Failed to create cluster handle: %v\n", err)
        return
    }
    defer cluster.Shutdown()

    err = cluster.ReadConfigFile("/etc/ceph/ceph.conf")
    if err != nil {
        fmt.Printf("Failed to read the configuration file: %v\n", err)
        return
    }

    err = cluster.Connect()
    if err != nil {
        fmt.Printf("Failed to connect to the cluster: %v\n", err)
        return
    }

    ioctx, err := cluster.OpenIOContext(poolname)
    if err != nil {
        fmt.Printf("Failed to open pool %s: %v\n", poolname, err)
        return
    }
    defer ioctx.Destroy()

    // max object size 4MiB
    buf := make([]byte, 4*1024*1024)
    _, err = ioctx.Read(objname, buf, 0)
    if err != nil {
        fmt.Printf("Failed to read object %s: %v\n", objname, err)
        return
    }

    err = ioutil.WriteFile(file_path, buf, 0644)
    if err != nil {
        fmt.Printf("Failed to save file: %v\n", err)
        return
    }

    fmt.Println("Object saved successfully.")
}
