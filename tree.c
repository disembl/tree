/***************
 Copyright (c) 2013, Matthew Levenstein
 All rights reserved.
 
 Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
 
 Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************/

/*
 * To conver to an append-only design, only the 'fwrite' in the '_insert' function
 * must be changed. All other writes (save for delete), are appended to the end.
 * Also, the root address must instead be written to and searched for at the end.
 */

#include <stdio.h>
#include <stdlib.h>    
#include <string.h>
#include <time.h>

#ifdef L_ctermidNOPE
#include <unistd.h>
#include <fcntl.h>
#define LOCK
#endif

typedef unsigned long long uint64_t;
typedef unsigned int uint32_t;
typedef unsigned char uint8_t;
typedef struct db db;

#define SIZEOF_LONG sizeof(uint64_t)
#define _HASH 33
#define _ORDER 99
#define _WIDTH 1+_HASH*_ORDER+SIZEOF_LONG*(_ORDER+1)
#define _DEPTH 10
#define _MAX 0xf4240

struct db{
        FILE* fp;
        unsigned char path[_DEPTH][_WIDTH];
        uint64_t node_addrs[_DEPTH];
#ifdef LOCK
        struct flock fl;
#endif
};

void to_big(unsigned char *, uint64_t);
uint64_t from_big(unsigned char *);
void node_split(db *, int, unsigned char);
void _insert(db *, unsigned char *, int, uint64_t, uint64_t, int);
void put(db *, unsigned char *, unsigned char *);
uint64_t search(db *, unsigned char *, int *);
unsigned char* get(db *, unsigned char *);
void db_init(db *, const char *);

#ifdef LOCK
int db_lock(db *);
int db_unlock(db *);
#endif

void db_close(db *);

#ifdef LOCK
int
db_lock(db* db) {
  db->fl.l_type   = F_WRLCK; 
  db->fl.l_whence = SEEK_SET; 
  db->fl.l_start  = 0;        
  db->fl.l_len    = 0;        
  db->fl.l_pid    = getpid();
  return fcntl((db->fp)->_file,F_SETLKW, &(db->fl));
}

int
db_unlock(db* db) {
  db->fl.l_type = F_UNLCK;
  fcntl((db->fp)->_file, F_SETLK, &(db->fl));
}
#endif

void
to_big(unsigned char* buf, uint64_t val) {
  int i;
  for( i=0; i<sizeof(uint64_t); ++i )
    buf[i] = (val >> (56-(i*8))) & 0xff;
}

uint64_t
from_big(unsigned char* buf) {
  uint64_t val = 0;
  int i;
  for( i=0; i<sizeof(uint64_t); ++i )
    val |= (uint64_t) buf[i] << (56-(i*8));
  return val;
}

void
node_split(db* db, int index, unsigned char isleaf)
{
  unsigned char* node = db->path[index];
  unsigned char* lnode = malloc(_WIDTH+1);
  unsigned char* rnode = malloc(_WIDTH+1);
  memset(lnode,0,_WIDTH+1);
  memset(rnode,0,_WIDTH+1);
  int split = (_HASH+SIZEOF_LONG)*(_ORDER>>1)+1;
  memcpy(lnode,node,split);
  rnode[0] = isleaf;
  isleaf ? 
  memcpy(rnode+1,node+split,_WIDTH-split) :
  memcpy(rnode+1,node+split+SIZEOF_LONG+_HASH,_WIDTH-(split+SIZEOF_LONG+_HASH));
  fseeko(db->fp,0,SEEK_END);
  uint64_t addr = ftello(db->fp);
  fwrite(rnode,1,_WIDTH,db->fp);
  to_big(lnode+split,addr);
  unsigned char* key = malloc(_HASH+1);
  memset(key,0,_HASH+1);
  memcpy(key,node+split+SIZEOF_LONG,_HASH);
  memcpy(db->path[index],lnode,_WIDTH);
  if( index > 0 ){
    _insert(db,key,index-1,db->node_addrs[index],addr,0);
  }
  else{
    unsigned char* root = malloc(_WIDTH+1);
    memset(root,0,_WIDTH+1);
  	root[0] = 0;
    to_big(root+1,db->node_addrs[0]);
    strncpy(root+1+SIZEOF_LONG,key,_HASH);
    to_big(root+1+SIZEOF_LONG+_HASH,addr);
    fseeko(db->fp,0,SEEK_END);
    addr = ftello(db->fp);
    fwrite(root,1,_WIDTH,db->fp);
    fseeko(db->fp,0,SEEK_SET);
    fwrite(&addr,1,SIZEOF_LONG,db->fp);
  }
}

void
_insert(db* db, unsigned char* key, int index, uint64_t addr, uint64_t rptr, int isleaf)
{
  if( _HASH > strlen(key) ){
    unsigned char* okey = key;
    key = malloc(_HASH+1);
    memset(key,0x61,_HASH+1);
    strncpy(key,okey,strlen(okey));
  }
  unsigned char* node = db->path[index];
  int i = SIZEOF_LONG+1;
  for( ; i<_WIDTH; i+=(_HASH+SIZEOF_LONG) ){
    if( node[i] == 0 ){
      i -= SIZEOF_LONG;
      to_big(node+i,addr);
      i += SIZEOF_LONG;
      strncpy(node+i,key,_HASH);
      if( !isleaf ){
      	i += _HASH;
      	to_big(node+i,rptr);
      }
      break;
    }
    if( !strncmp(node+i,key,_HASH) ){
      if( isleaf ){
        i -= SIZEOF_LONG;
        to_big(node+i,addr);
      }
      break;
    }
    if( strncmp(node+i,key,_HASH) > 0 ){
      unsigned char* nnode = malloc(_WIDTH+1);
      memset(nnode,0,_WIDTH+1);
      i -= SIZEOF_LONG;
      int j;
      for( j=0; j<i; ++j ){
        nnode[j] = node[j];
	    }
	  	to_big(nnode+i,addr);
      i += SIZEOF_LONG;
      strncpy(nnode+i,key,_HASH);
      i += _HASH;
      if( !isleaf ){
      	to_big(nnode+i,rptr);
      	i += SIZEOF_LONG;
      	j += SIZEOF_LONG;
      }
      for( ; i<_WIDTH; ++i, ++j ){
      	nnode[i] = node[j];
      }
      memcpy(db->path[index],nnode,_WIDTH);
      break;
    }
  }
  if( node[(_WIDTH-(_HASH+SIZEOF_LONG))] != 0 ){
    isleaf ?
    node_split(db,index,1):
    node_split(db,index,0);
  }
  fseeko(db->fp,db->node_addrs[index],SEEK_SET);
  fwrite(db->path[index],1,_WIDTH,db->fp);
}

uint64_t
search(db* db, unsigned char* key, int* r_index)
{
  if( _HASH > strlen(key) ){
    unsigned char* okey = key;
    key = malloc(_HASH+1);
    memset(key,0x61,_HASH+1);
    strncpy(key,okey,strlen(okey));
  }
  uint64_t r_addr;
  int i = SIZEOF_LONG+1;
  unsigned char isleaf;
  int index = 0;
  fseeko(db->fp,0,SEEK_SET);
  fread(&r_addr,1,SIZEOF_LONG,db->fp);
  fseeko(db->fp,r_addr,SEEK_SET);
  fread(db->path[index],1,_WIDTH,db->fp);
  db->node_addrs[index] = r_addr;
search:
  isleaf = db->path[index][0];
  for( ; i<_WIDTH; i+=(_HASH+SIZEOF_LONG) ){
    if( !strncmp(db->path[index]+i,key,_HASH) ){
      if( isleaf ){
        *r_index = index;
        i -= SIZEOF_LONG;
        uint64_t cindex = from_big(db->path[index]+i);
        fseeko(db->fp,cindex,SEEK_SET);
        unsigned char check;
        fread(&check,1,1,db->fp);
        if( check == 0 ){
          return 1;
        }
        return 0;
      }
      if( index >= _DEPTH ){
        *r_index = 0;
        return -1;
      }
      i += _HASH;
      uint64_t addr = from_big(db->path[index]+i);
      fseeko(db->fp,addr,SEEK_SET);
      ++index;
      fread(db->path[index],1,_WIDTH,db->fp);
      db->node_addrs[index] = addr;
      i = SIZEOF_LONG+1;
      goto search;
    }
    if( strncmp(db->path[index]+i,key,_HASH) > 0 ||
    		db->path[index][i] == 0 ){
      if( isleaf ){
        *r_index = index;
        return 1;
      }
      if( index >= _DEPTH ){
        *r_index = 0;
        return -1;
      }
      i -= SIZEOF_LONG;
      uint64_t addr = from_big(db->path[index]+i);
      fseeko(db->fp,addr,SEEK_SET);
      ++index;
      fread(db->path[index],1,_WIDTH,db->fp);
      db->node_addrs[index] = addr;
      i = SIZEOF_LONG+1;
      goto search;
    }
	}
}

void
put(db* db, unsigned char* key, unsigned char* value)
{
	int index;
  uint64_t ret;
#ifdef LOCK
  if( db_lock(db) == -1 ){
    perror("fcntl");
    return;
  }
  else{
#endif
    if( (ret = search(db,key,&index)) > 0 ){
      uint64_t k_len = strlen(key);
      uint64_t v_len = strlen(value);
      if( k_len+v_len > _MAX ){ return; }
      uint64_t n_len = k_len+v_len+SIZEOF_LONG+SIZEOF_LONG+1;
      unsigned char* nnode = malloc(n_len+1);
      unsigned char* ptr = nnode;
      memset(nnode,0,n_len+1);
      nnode[0] = 1;
      to_big(ptr+1,k_len);
      strncpy(ptr+SIZEOF_LONG+1,key,k_len);
      to_big(ptr+SIZEOF_LONG+k_len+1,v_len);
      strncpy(ptr+SIZEOF_LONG+k_len+SIZEOF_LONG+1,value,v_len);
      fseeko(db->fp,0,SEEK_END);
      uint64_t addr = ftello(db->fp);
      fwrite(nnode,1,n_len,db->fp);
      _insert(db,key,index,addr,0,1);
    }
#ifdef LOCK
    if( db_unlock(db) == -1 ){
      perror("fcntl");
      return;
    }
  }
#endif
  fflush(db->fp);
}

unsigned char*
get(db* db, unsigned char* key)
{
  int index;
  if( _HASH > strlen(key) ){
    unsigned char* okey = key;
    key = malloc(_HASH+1);
    memset(key,0x61,_HASH+1);
    strncpy(key,okey,strlen(okey));
  }
  if( !search(db,key,&index) ){
    int i = SIZEOF_LONG+1;
    for( ; i<_WIDTH; i+=(SIZEOF_LONG+_HASH) ){
      if( !strncmp(db->path[index]+i,key,_HASH) ){
        i -= SIZEOF_LONG;
        uint64_t addr = from_big(db->path[index]+i);
        fseeko(db->fp,addr,SEEK_SET);
        unsigned char exists;
        unsigned char k_len[SIZEOF_LONG];
        unsigned char v_len[SIZEOF_LONG];
        fread(&exists,1,1,db->fp);
        if( !exists ){
          return NULL;
        }
        fread(k_len,1,SIZEOF_LONG,db->fp);
        uint64_t k_lenb = from_big(k_len);
        unsigned char* k = malloc(k_lenb);
        memset(k,0,k_lenb);
        fread(k,1,k_lenb,db->fp);
        fread(v_len,1,SIZEOF_LONG,db->fp);
        uint64_t v_lenb = from_big(v_len);
        unsigned char* v = malloc(v_lenb);
        memset(v,0,v_lenb);
        fread(v,1,v_lenb,db->fp);
        return v;
      }
    }
    return NULL;
  }
  return NULL;
}

void
delete(db* db, unsigned char* key)
{
  int index;
  if( _HASH > strlen(key) ){
    unsigned char* okey = key;
    key = malloc(_HASH+1);
    memset(key,0x61,_HASH+1);
    strncpy(key,okey,strlen(okey));
  }
  if( !search(db,key,&index) ){
    int i = SIZEOF_LONG+1;
    for( ; i<_WIDTH; i+=(SIZEOF_LONG+_HASH) ){
      if( !strncmp(db->path[index]+i,key,_HASH) ){
        i -= SIZEOF_LONG;
        unsigned char del = 0;
        uint64_t addr = from_big(db->path[index]+i);
        fseeko(db->fp,addr,SEEK_SET);
        fwrite(&del,1,1,db->fp);
      }
    }
  }
}

void
db_init(db* db, const char* name)
{
  uint64_t addr;
  unsigned char* zero = malloc(_WIDTH);
  memset(zero,0,_WIDTH);
  zero[0] = 1;
  db->fp = fopen(name,"rb+");
  if( !db->fp ){
    db->fp = fopen(name,"wb+");
    addr = SIZEOF_LONG;
    fseeko(db->fp,0,SEEK_SET);
    fwrite(&addr,SIZEOF_LONG,1,db->fp);
    fwrite(zero,1,_WIDTH,db->fp);
  }
}

void
db_close(db* db)
{
  fclose(db->fp);
}

/*** function for testing ***/

char *random_str() {
  int i;
  char *alphabet = "abcdefghijklmnopqrstuvwxyz";
  char *string = malloc(33);
  for (i=0; i<32; i++) {
    string[i] = alphabet[rand()%26];
  }
  string[i] = 0;
  return string;
}

/***************************/

int
main(void)
{
  db new;
  db_init(&new, "test");
  put(&new,"hello","world");
  char* value = get(&new,"hello");
  puts(value);
  db_close(&new);
  return 0; 
}
