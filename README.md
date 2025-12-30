# Block Serialization Byte

## Overall Structure
```text
+--------------------+-------------------+------------------+----------+
|   Needle Header    |       Data        |  Needle Footer   | Padding  |
|      29 bytes      |    N bytes        |     8 bytes     | 0–7 byte |
+--------------------+-------------------+------------------+----------+
```

### Needle Header
```text
+---------------+----------+----------+--------------+------+--------+
| MagicHeader   | Cookie   | Key      | AlternateKey | Flag | Size   |
| 4 bytes       | 8 bytes  | 8 bytes  | 4 bytes      | 1    | 4 bytes|
+---------------+----------+----------+--------------+------+--------+
```

### Data
```text
+--------------------+
|      Data          |
|   Size bytes       |
+--------------------+
```

### Needle Footer
```text
+-----------+-------------+
| Checksum  | MagicFooter |
| 4 bytes   | 4 bytes     |
+-----------+-------------+
```

# 小寫入batch test
- Volume(Haystack) v.s. Disk file
---

## `sync.Pool`
### Write
![](img/sw.png)
---
### Reader
![](img/sr.png)

## 使用自訂義Buffer Pool取代原先單純`sync.Pool`
![](img/swp.png)

## 使用 go trace 查看 Event timelines for running goroutines

### Volume
![](img/volume.png)
### IO
![](img/go_test_os.png)

---

![](img/1.png)