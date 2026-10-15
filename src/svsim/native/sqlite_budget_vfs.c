/* Loadable SQLite VFS that puts a hard pre-write cap around rollback journals.
 * It uses only sqlite3ext.h dispatch pointers, so it runs inside the SQLite
 * library that loaded it (including Python's builtin sqlite module). */
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

/* Older system headers (e.g. 3.26) lack this flag; the extension runs inside the
 * loading library, so its value is valid whenever that library is >= 3.30
 * (checked in sqlite3_svsimdisk_init). */
#ifndef SQLITE_DIRECTONLY
#define SQLITE_DIRECTONLY 0x000080000
#endif
#define VFS_NAME "svsim-budget-v1"
#define PATH_LIMIT 4096

typedef struct BudgetDb BudgetDb;
typedef struct BudgetFile BudgetFile;
struct BudgetDb {
  char *path;
  sqlite3_int64 mainCap, sideCap, sideGrowth;
  int active, refs, preparedRefs;
  BudgetDb *next;
};
struct BudgetFile {
  sqlite3_file base;
  sqlite3_file *real;
  BudgetDb *db;
  int isMain, writable;
  sqlite3_int64 sideHigh;
};


static sqlite3_vfs gVfs;
static sqlite3_vfs *gParent;
static sqlite3_mutex *gMutex;
static BudgetDb *gDbs;
static sqlite3_int64 gDenied;

static void lockState(void){ if(gMutex) sqlite3_mutex_enter(gMutex); }
static void unlockState(void){ if(gMutex) sqlite3_mutex_leave(gMutex); }
static BudgetDb *findDb(const char *path){
  BudgetDb *p;
  for(p=gDbs;p;p=p->next) if(strcmp(p->path,path)==0) return p;
  return 0;
}
static int accountCount(void){ BudgetDb*p; int n=0; for(p=gDbs;p;p=p->next)n++; return n; }
static void freeDbIfUnused(BudgetDb *d){
  BudgetDb **q;
  if(!d || d->refs || d->preparedRefs) return;
  q=&gDbs; while(*q && *q!=d) q=&(*q)->next;
  if(*q){ *q=d->next; sqlite3_free(d->path); sqlite3_free(d); }
}
static char *fullPath(const char *z){
  char *out;
  if(!z) return 0;
  out=(char*)sqlite3_malloc(PATH_LIMIT);
  if(!out) return 0;
  if(gParent->xFullPathname(gParent,z,PATH_LIMIT,out)!=SQLITE_OK){ sqlite3_free(out); return 0; }
  return out;
}
static int suffix(const char *s,const char *tail){
  size_t a,b; if(!s||!tail) return 0; a=strlen(s); b=strlen(tail);
  return a>=b && strcmp(s+a-b,tail)==0;
}
static void denied(void){ lockState(); gDenied++; unlockState(); }
#ifdef SVSIM_TEST_FAULTS
/* Test-only deterministic fault point, compiled in only with -DSVSIM_TEST_FAULTS
 * (SQLiteGuard adds the flag when SVSIM_VFS_TEST_FAULTS=1). A production binary
 * contains none of this code. Kinds: 1 journal xSync, 2 main-db xSync, 3 journal
 * delete (the commit point of a rollback-journal transaction). Actions: 1 fail the
 * Nth call with an I/O error, 2 _exit before the real operation, 3 _exit after it. */
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
/* Test-only durable filesystem images. The parent restores these after a
 * process death, discarding unsynced writes and directory changes. Images live
 * outside the budgeted namespace and are never part of a production build. */
#define POWER_FILES 32
#define POWER_FILE_LIMIT (16*1024*1024)
static struct { char path[PATH_LIMIT]; int present; } gPowerFiles[POWER_FILES];
static char gPowerRoot[PATH_LIMIT], gPowerFilter[PATH_LIMIT];
static int gPowerCount, gPowerActive;
static int powerManifest(void){
  char temp[PATH_LIMIT], target[PATH_LIMIT]; FILE *out; int i;
  if(snprintf(temp,sizeof(temp),"%s/manifest.tmp",gPowerRoot)>=(int)sizeof(temp) ||
     snprintf(target,sizeof(target),"%s/manifest.tsv",gPowerRoot)>=(int)sizeof(target))return SQLITE_IOERR;
  out=fopen(temp,"wb"); if(!out)return SQLITE_IOERR;
  for(i=0;i<gPowerCount;i++)
    if(fprintf(out,"%d\t%d\t%s\n",i,gPowerFiles[i].present,gPowerFiles[i].path)<0){fclose(out);return SQLITE_IOERR;}
  if(fclose(out)!=0 || rename(temp,target)!=0)return SQLITE_IOERR;
  return SQLITE_OK;
}
static int powerImage(const char *path,int deleted){
  int i; char image[PATH_LIMIT]; char bytes[16384]; FILE *in,*out; long size; size_t n;
  if(!gPowerActive || !path || !strstr(path,gPowerFilter))return SQLITE_OK;
  if(strlen(path)>=PATH_LIMIT || strchr(path,'\n') || strchr(path,'\t'))return SQLITE_IOERR;
  for(i=0;i<gPowerCount;i++)if(!strcmp(gPowerFiles[i].path,path))break;
  if(i==gPowerCount){
    if(gPowerCount>=POWER_FILES)return SQLITE_FULL;
    strcpy(gPowerFiles[i].path,path);gPowerFiles[i].present=0;gPowerCount++;
  }
  if(snprintf(image,sizeof(image),"%s/%d.bin",gPowerRoot,i)>=(int)sizeof(image))return SQLITE_IOERR;
  if(deleted){gPowerFiles[i].present=0;return powerManifest();}
  in=fopen(path,"rb");
  if(!in){
    if(errno==ENOENT){gPowerFiles[i].present=0;return powerManifest();}
    return SQLITE_IOERR;
  }
  if(fseek(in,0,SEEK_END)!=0 || (size=ftell(in))<0 || size>POWER_FILE_LIMIT || fseek(in,0,SEEK_SET)!=0){
    fclose(in);return SQLITE_FULL;
  }
  out=fopen(image,"wb");if(!out){fclose(in);return SQLITE_IOERR;}
  while((n=fread(bytes,1,sizeof(bytes),in))>0)
    if(fwrite(bytes,1,n,out)!=n){fclose(in);fclose(out);return SQLITE_IOERR;}
  if(ferror(in)){fclose(in);fclose(out);return SQLITE_IOERR;}
  fclose(in);if(fclose(out)!=0)return SQLITE_IOERR;
  gPowerFiles[i].present=1;return powerManifest();
}
static void powerArmFn(sqlite3_context*ctx,int argc,sqlite3_value**argv){
  const char *filter=(const char*)sqlite3_value_text(argv[0]);
  const char *root=(const char*)sqlite3_value_text(argv[1]);
  BudgetDb *d; char journal[PATH_LIMIT]; struct stat info; int rc=SQLITE_OK;(void)argc;
  if(!filter || !filter[0] || !root || root[0]!='/' || strlen(filter)>=PATH_LIMIT ||
     strlen(root)>PATH_LIMIT-32 || stat(root,&info)!=0 || !S_ISDIR(info.st_mode)){
    sqlite3_result_error(ctx,"bad power model arguments",-1);return;
  }
  gPowerActive=0;gPowerCount=0;strcpy(gPowerFilter,filter);strcpy(gPowerRoot,root);gPowerActive=1;
  for(d=gDbs;d && rc==SQLITE_OK;d=d->next){
    if(!strstr(d->path,filter))continue;
    rc=powerImage(d->path,0);
    if(rc==SQLITE_OK && snprintf(journal,sizeof(journal),"%s-journal",d->path)<(int)sizeof(journal))
      rc=powerImage(journal,0);
  }
  if(rc!=SQLITE_OK){gPowerActive=0;sqlite3_result_error(ctx,"cannot seed power model",-1);return;}
  sqlite3_result_int(ctx,gPowerCount);
}
static void powerDisarmFn(sqlite3_context*ctx,int argc,sqlite3_value**argv){
  (void)argc;(void)argv;gPowerActive=0;sqlite3_result_int(ctx,1);
}
static char gFaultFilter[PATH_LIMIT];
static int gFaultKind, gFaultNth, gFaultAction, gFaultCount;
static int faultPoint(const char *mainPath,int kind,int after){
  int hit=0;
  lockState();
  if(gFaultKind==kind && gFaultFilter[0] && mainPath && strstr(mainPath,gFaultFilter)){
    if(!after) gFaultCount++;
    if(gFaultCount==gFaultNth) hit=gFaultAction;
  }
  unlockState();
  if(!after && hit==1) return 1;
  if(!after && hit==2) _exit(0);
  if(after && hit==3) _exit(0);
  return 0;
}
static void faultArmFn(sqlite3_context*ctx,int argc,sqlite3_value**argv){
  const char *filter,*kind,*action; (void)argc;
  filter=(const char*)sqlite3_value_text(argv[0]); kind=(const char*)sqlite3_value_text(argv[1]);
  action=(const char*)sqlite3_value_text(argv[3]);
  if(!filter||!kind||!action||strlen(filter)>=PATH_LIMIT){sqlite3_result_error(ctx,"bad fault arguments",-1);return;}
  lockState();
  strcpy(gFaultFilter,filter);
  gFaultKind=!strcmp(kind,"journal-sync")?1:!strcmp(kind,"main-sync")?2:!strcmp(kind,"journal-delete")?3:0;
  gFaultAction=!strcmp(action,"fail")?1:!strcmp(action,"kill-before")?2:!strcmp(action,"kill-after")?3:0;
  gFaultNth=sqlite3_value_int(argv[2]); gFaultCount=0;
  unlockState();
  sqlite3_result_int(ctx,gFaultKind&&gFaultAction?1:0);
}
static void faultDisarmFn(sqlite3_context*ctx,int argc,sqlite3_value**argv){
  (void)argc;(void)argv; lockState(); gFaultKind=0; gFaultFilter[0]=0; unlockState(); sqlite3_result_int(ctx,1);
}
#endif

static int guardWrite(BudgetFile *f, sqlite3_int64 end, sqlite3_int64 oldSize){
  sqlite3_int64 increase=0;
  BudgetDb *d=f->db;
  if(!f->writable || !d){ denied(); return SQLITE_READONLY; }
  lockState();
  if(!d->active){ gDenied++; unlockState(); return SQLITE_FULL; }
  if(f->isMain){
    if(end>d->mainCap){ gDenied++; unlockState(); return SQLITE_FULL; }
  } else {
    if(end>f->sideHigh){ increase=end-f->sideHigh; }
    if(increase>0 && d->sideGrowth + increase>d->sideCap){
      gDenied++; unlockState(); return SQLITE_FULL;
    }
    d->sideGrowth += increase;
    if(end>f->sideHigh) f->sideHigh=end;
  }
  (void)oldSize;
  unlockState();
  return SQLITE_OK;
}
static int bfClose(sqlite3_file *p){
  BudgetFile *f=(BudgetFile*)p; int rc=f->real->pMethods->xClose(f->real);
  if(f->db){
    lockState();
    f->db->refs--;
    freeDbIfUnused(f->db);
    unlockState();
  }
  p->pMethods=0;
  return rc;
}
static int bfRead(sqlite3_file*p,void*b,int n,sqlite3_int64 o){return ((BudgetFile*)p)->real->pMethods->xRead(((BudgetFile*)p)->real,b,n,o);}
static int bfWrite(sqlite3_file*p,const void*b,int n,sqlite3_int64 o){
  BudgetFile*f=(BudgetFile*)p; sqlite3_int64 end=o+n, size=0; int rc;
  if(o<0 || n<0 || end<o) return SQLITE_IOERR_WRITE;
  f->real->pMethods->xFileSize(f->real,&size);
  rc=guardWrite(f,end,size); if(rc!=SQLITE_OK) return rc;
  return f->real->pMethods->xWrite(f->real,b,n,o);
}
static int bfTruncate(sqlite3_file*p,sqlite3_int64 n){
  BudgetFile*f=(BudgetFile*)p; sqlite3_int64 size=0; int rc;
  if(n<0) return SQLITE_IOERR_TRUNCATE;
  f->real->pMethods->xFileSize(f->real,&size);
  if(f->isMain && n<size){
    if(!f->db){denied();return SQLITE_READONLY;}
    lockState();
    if(!f->db->active){gDenied++;unlockState();return SQLITE_FULL;}
    unlockState();
  }
  if(n>size){ rc=guardWrite(f,n,size); if(rc!=SQLITE_OK) return rc; }
  return f->real->pMethods->xTruncate(f->real,n);
}
static int bfSync(sqlite3_file*p,int flags){
  BudgetFile*f=(BudgetFile*)p; int rc;
#ifdef SVSIM_TEST_FAULTS
  if(f->db && faultPoint(f->db->path,f->isMain?2:1,0)) return SQLITE_IOERR_FSYNC;
#endif
  rc=f->real->pMethods->xSync(f->real,flags);
#ifdef SVSIM_TEST_FAULTS
  if(rc==SQLITE_OK && f->db){
    char journal[PATH_LIMIT];const char *path=f->db->path;
    if(!f->isMain){
      if(snprintf(journal,sizeof(journal),"%s-journal",path)>=(int)sizeof(journal))return SQLITE_IOERR_FSYNC;
      path=journal;
    }
    if(powerImage(path,0)!=SQLITE_OK)return SQLITE_IOERR_FSYNC;
    faultPoint(f->db->path,f->isMain?2:1,1);
  }
#endif
  return rc;
}
static int bfSize(sqlite3_file*p,sqlite3_int64*n){return ((BudgetFile*)p)->real->pMethods->xFileSize(((BudgetFile*)p)->real,n);}
static int bfLock(sqlite3_file*p,int l){return ((BudgetFile*)p)->real->pMethods->xLock(((BudgetFile*)p)->real,l);}
static int bfUnlock(sqlite3_file*p,int l){return ((BudgetFile*)p)->real->pMethods->xUnlock(((BudgetFile*)p)->real,l);}
static int bfCheck(sqlite3_file*p,int*r){return ((BudgetFile*)p)->real->pMethods->xCheckReservedLock(((BudgetFile*)p)->real,r);}
static int bfControl(sqlite3_file*p,int op,void*a){
  if(op==SQLITE_FCNTL_PRAGMA && a){
    char **args=(char**)a;
    if(args[1] && strcasecmp(args[1],"journal_mode")==0 && args[2] && strcasecmp(args[2],"delete")!=0){
      denied(); return SQLITE_ERROR;
    }
  }
  return ((BudgetFile*)p)->real->pMethods->xFileControl(((BudgetFile*)p)->real,op,a);
}
static int bfSector(sqlite3_file*p){return ((BudgetFile*)p)->real->pMethods->xSectorSize(((BudgetFile*)p)->real);}
static int bfDevice(sqlite3_file*p){return ((BudgetFile*)p)->real->pMethods->xDeviceCharacteristics(((BudgetFile*)p)->real);}
static int bfShmMap(sqlite3_file*p,int i,int n,int e,void volatile **v){(void)p;(void)i;(void)n;(void)e;*v=0;return SQLITE_IOERR_SHMMAP;}
static int bfShmLock(sqlite3_file*p,int o,int n,int f){(void)p;(void)o;(void)n;(void)f;return SQLITE_IOERR_SHMLOCK;}
static void bfShmBarrier(sqlite3_file*p){(void)p;}
static int bfShmUnmap(sqlite3_file*p,int d){(void)p;(void)d;return SQLITE_OK;}
static int bfFetch(sqlite3_file*p,sqlite3_int64 o,int n,void **v){(void)p;(void)o;(void)n;*v=0;return SQLITE_OK;}
static int bfUnfetch(sqlite3_file*p,sqlite3_int64 o,void*v){(void)p;(void)o;(void)v;return SQLITE_OK;}
static const sqlite3_io_methods gIo={3,bfClose,bfRead,bfWrite,bfTruncate,bfSync,bfSize,bfLock,bfUnlock,bfCheck,bfControl,bfSector,bfDevice,bfShmMap,bfShmLock,bfShmBarrier,bfShmUnmap,bfFetch,bfUnfetch};

static int vOpen(sqlite3_vfs*v,const char*z,sqlite3_file*p,int flags,int*outFlags){
  BudgetFile*f=(BudgetFile*)p; char *path=fullPath(z); int isMain=(flags&SQLITE_OPEN_MAIN_DB)!=0;
  int isJournal=(flags&(SQLITE_OPEN_MAIN_JOURNAL|SQLITE_OPEN_TEMP_JOURNAL|SQLITE_OPEN_SUBJOURNAL))!=0;
  int writable=(flags&SQLITE_OPEN_READWRITE)!=0;
  int rc; size_t align=(size_t)gParent->szOsFile;
  if(!path) return SQLITE_CANTOPEN;
  /* WAL, shared-memory, temp and unrelated writable files are unsupported. */
  if((writable && !isMain && !isJournal) || suffix(path,"-wal") || suffix(path,"-shm")){
    sqlite3_free(path); return SQLITE_CANTOPEN;
  }
  memset(f,0,sizeof(*f));
  f->real=(sqlite3_file*)(((uintptr_t)(f+1)+7u)&~(uintptr_t)7u);
  memset(f->real,0,align);
  rc=gParent->xOpen(gParent,z,f->real,flags,outFlags);
  if(rc!=SQLITE_OK){sqlite3_free(path);return rc;}
  f->isMain=isMain; f->writable=writable;
  if(isMain){
    lockState();
    f->db=findDb(path);
    if(!f->db){
      if(accountCount()<256){
        f->db=(BudgetDb*)sqlite3_malloc(sizeof(BudgetDb));
        if(f->db){
          memset(f->db,0,sizeof(*f->db)); f->db->path=sqlite3_mprintf("%s",path);
          if(!f->db->path){sqlite3_free(f->db);f->db=0;}
          else{f->db->next=gDbs;gDbs=f->db;}
        }
      }
    }
    if(f->db) f->db->refs++;
    unlockState();
    if(!f->db){f->real->pMethods->xClose(f->real);sqlite3_free(path);return SQLITE_CANTOPEN;}
  } else if(isJournal){
    size_t n=strlen(path); char *mainPath=0;
    if(suffix(path,"-journal")){ path[n-8]=0; mainPath=path; }
    if(mainPath){ lockState(); f->db=findDb(mainPath); if(f->db)f->db->refs++; unlockState(); }
    if(f->db){
      sqlite3_int64 sz=0; f->real->pMethods->xFileSize(f->real,&sz); f->sideHigh=sz;
      if(sz>0){ lockState(); f->db->sideGrowth += sz; unlockState(); }
      lockState(); rc=f->db->sideGrowth>f->db->sideCap?SQLITE_FULL:SQLITE_OK; unlockState();
      if(rc!=SQLITE_OK){
        f->real->pMethods->xClose(f->real);
        lockState(); f->db->refs--; freeDbIfUnused(f->db); unlockState();
        sqlite3_free(path); return rc;
      }
    }
  }
  sqlite3_free(path);
  f->base.pMethods=&gIo;
  return SQLITE_OK;
}
static int vDelete(sqlite3_vfs*v,const char*z,int syncDir){
  char *path=fullPath(z); BudgetDb*d=0; int active=0;
  if(path){
    lockState(); d=findDb(path); if(d)active=d->active; unlockState();
    sqlite3_free(path);
    if(d && !active){denied();return SQLITE_FULL;}
  }
#ifdef SVSIM_TEST_FAULTS
  {
    char *jp=fullPath(z);
    if(jp && suffix(jp,"-journal")){
      jp[strlen(jp)-8]=0;
      if(faultPoint(jp,3,0)){sqlite3_free(jp);return SQLITE_IOERR_DELETE;}
      {
        int rc=gParent->xDelete(gParent,z,syncDir);
        if(rc==SQLITE_OK && syncDir && powerImage(z,1)!=SQLITE_OK)rc=SQLITE_IOERR_DELETE;
        if(rc==SQLITE_OK)faultPoint(jp,3,1);
        sqlite3_free(jp);return rc;
      }
    }
    sqlite3_free(jp);
  }
#endif
  return gParent->xDelete(gParent,z,syncDir);
}
static int vAccess(sqlite3_vfs*v,const char*z,int f,int*r){return gParent->xAccess(gParent,z,f,r);}
static int vFull(sqlite3_vfs*v,const char*z,int n,char*out){return gParent->xFullPathname(gParent,z,n,out);}
static void *vDlOpen(sqlite3_vfs*v,const char*z){return gParent->xDlOpen(gParent,z);}
static void vDlError(sqlite3_vfs*v,int n,char*z){gParent->xDlError(gParent,n,z);}
static void (*vDlSym(sqlite3_vfs*v,void*h,const char*z))(void){return gParent->xDlSym(gParent,h,z);}
static void vDlClose(sqlite3_vfs*v,void*h){gParent->xDlClose(gParent,h);}
static int vRandom(sqlite3_vfs*v,int n,char*z){return gParent->xRandomness(gParent,n,z);}
static int vSleep(sqlite3_vfs*v,int n){return gParent->xSleep(gParent,n);}
static int vTime(sqlite3_vfs*v,double*t){return gParent->xCurrentTime(gParent,t);}
static int vLast(sqlite3_vfs*v,int n,char*z){return gParent->xGetLastError(gParent,n,z);}
static int vTime64(sqlite3_vfs*v,sqlite3_int64*t){return gParent->iVersion>=2?gParent->xCurrentTimeInt64(gParent,t):SQLITE_ERROR;}

static char *connectionPath(sqlite3_context *ctx){
  sqlite3 *db=sqlite3_context_db_handle(ctx); const char*z=sqlite3_db_filename(db,"main");
  return z?fullPath(z):0;
}
static void prepareFn(sqlite3_context*ctx,int argc,sqlite3_value**argv){
  const unsigned char *z; char *path; sqlite3_int64 mainCap,sideCap; BudgetDb*d;
  if(argc!=3 || sqlite3_value_type(argv[0])!=SQLITE_TEXT ||
     sqlite3_value_type(argv[1])!=SQLITE_INTEGER || sqlite3_value_type(argv[2])!=SQLITE_INTEGER){
    sqlite3_result_error(ctx,"absolute path and integer guard caps required",-1);return;
  }
  z=sqlite3_value_text(argv[0]); mainCap=sqlite3_value_int64(argv[1]); sideCap=sqlite3_value_int64(argv[2]);
  if(!z || mainCap<=0 || sideCap<0){sqlite3_result_error(ctx,"invalid prepared guard",-1);return;}
  path=fullPath((const char*)z); if(!path){sqlite3_result_error(ctx,"cannot canonicalize prepared database path",-1);return;}
  lockState(); d=findDb(path);
  if(d && (d->active || d->preparedRefs)){
    unlockState();sqlite3_free(path);sqlite3_result_error(ctx,"database path already has an active or prepared guard",-1);return;
  }
  if(!d){
    if(accountCount()>=256){unlockState();sqlite3_free(path);sqlite3_result_error(ctx,"guard account table is full",-1);return;}
    d=(BudgetDb*)sqlite3_malloc(sizeof(*d));
    if(d){memset(d,0,sizeof(*d));d->path=sqlite3_mprintf("%s",path);}
    if(!d || !d->path){if(d)sqlite3_free(d);unlockState();sqlite3_free(path);sqlite3_result_error_nomem(ctx);return;}
    d->next=gDbs;gDbs=d;
  }
  d->mainCap=mainCap;d->sideCap=sideCap;d->sideGrowth=0;d->active=1;d->preparedRefs++;
  unlockState();sqlite3_free(path);sqlite3_result_int(ctx,1);
}
static void unprepareFn(sqlite3_context*ctx,int argc,sqlite3_value**argv){
  const unsigned char*z; char*path; BudgetDb*d;
  if(argc!=1 || sqlite3_value_type(argv[0])!=SQLITE_TEXT){sqlite3_result_error(ctx,"database path required",-1);return;}
  z=sqlite3_value_text(argv[0]);path=z?fullPath((const char*)z):0;
  if(!path){sqlite3_result_error(ctx,"cannot canonicalize prepared database path",-1);return;}
  lockState();d=findDb(path);
  if(d && d->preparedRefs>0){d->preparedRefs--;freeDbIfUnused(d);}
  unlockState();sqlite3_free(path);sqlite3_result_int(ctx,1);
}
static void configFn(sqlite3_context*ctx,int argc,sqlite3_value**argv){
  sqlite3_int64 mainCap,sideCap,size=0; int active; char *path; BudgetDb*d; sqlite3_file *mf=0;
  if(argc!=3 || sqlite3_value_type(argv[0])!=SQLITE_INTEGER || sqlite3_value_type(argv[1])!=SQLITE_INTEGER || sqlite3_value_type(argv[2])!=SQLITE_INTEGER){sqlite3_result_error(ctx,"integer guard configuration required",-1);return;}
  mainCap=sqlite3_value_int64(argv[0]); sideCap=sqlite3_value_int64(argv[1]); active=sqlite3_value_int(argv[2]);
  if(mainCap<0 || sideCap<0 || (active!=0&&active!=1)){sqlite3_result_error(ctx,"invalid guard configuration",-1);return;}
  path=connectionPath(ctx); if(!path){sqlite3_result_error(ctx,"main database path unavailable",-1);return;}
  lockState(); d=findDb(path);
  if(!d){unlockState();sqlite3_free(path);sqlite3_result_error(ctx,"database is not open through guarded VFS",-1);return;}
  if(sqlite3_file_control(sqlite3_context_db_handle(ctx),"main",SQLITE_FCNTL_FILE_POINTER,&mf)==SQLITE_OK && mf){
    mf->pMethods->xFileSize(mf,&size);
  }
  if(active && (mainCap<size || mainCap==0)){
    unlockState();sqlite3_free(path);sqlite3_result_error(ctx,"guard caps are below existing database charges",-1);return;
  }
  /* Each inactive-to-active transition starts a fresh admitted operation.
   * Growth remains cumulative across journal reopenings during that window. */
  if(active && !d->active) d->sideGrowth=0;
  if(active && d->sideGrowth>sideCap){
    unlockState();sqlite3_free(path);sqlite3_result_error(ctx,"guard caps are below existing database charges",-1);return;
  }
  d->mainCap=mainCap; d->sideCap=sideCap; d->active=active;
  unlockState(); sqlite3_free(path);
  sqlite3_result_int(ctx,1);
}
static void statusFn(sqlite3_context*ctx,int argc,sqlite3_value**argv){
  char *path=connectionPath(ctx), *json; BudgetDb*d; (void)argc;(void)argv;
  if(!path){sqlite3_result_error(ctx,"main database path unavailable",-1);return;}
  lockState(); d=findDb(path);
  if(!d){unlockState();sqlite3_free(path);sqlite3_result_error(ctx,"database is not open through guarded VFS",-1);return;}
  json=sqlite3_mprintf("{\"available\":true,\"sqlite_version\":\"%s\",\"active\":%s,\"main_cap_bytes\":%lld,\"side_growth_cap_bytes\":%lld,\"side_growth_bytes\":%lld,\"denied_writes\":%lld}",sqlite3_libversion(),d->active?"true":"false",(long long)d->mainCap,(long long)d->sideCap,(long long)d->sideGrowth,(long long)gDenied);
  unlockState();sqlite3_free(path);sqlite3_result_text(ctx,json,-1,sqlite3_free);
}
static int guardAuthorizer(void *unused,int action,const char *arg1,const char *arg2,
                           const char *database,const char *trigger){
  (void)unused;(void)database;(void)trigger;
  if(action==SQLITE_PRAGMA && arg1 && arg2 &&
     strcasecmp(arg1,"journal_mode")==0 && strcasecmp(arg2,"delete")!=0){
    denied(); return SQLITE_DENY;
  }
  return SQLITE_OK;
}

/* Read both the compile-time header identity and the loaded dispatch library.
 * Header and runtime versions may differ: SQLite supports compatible headers.
 * The caller compares dispatch identity to that of its actual connection. */
static void identityFn(sqlite3_context*ctx,int argc,sqlite3_value**argv){
  const char *key, *value=0;
  if(argc!=1 || sqlite3_value_type(argv[0])!=SQLITE_TEXT){
    sqlite3_result_error(ctx,"identity key required",-1);return;
  }
  key=(const char*)sqlite3_value_text(argv[0]);
  if(!strcmp(key,"runtime_version"))value=sqlite3_libversion();
  else if(!strcmp(key,"runtime_source_id"))value=sqlite3_sourceid();
  else if(!strcmp(key,"header_version"))value=SQLITE_VERSION;
  else if(!strcmp(key,"header_source_id"))value=SQLITE_SOURCE_ID;
  else {sqlite3_result_error(ctx,"unknown identity key",-1);return;}
  sqlite3_result_text(ctx,value,-1,SQLITE_TRANSIENT);
}
static void compileOptionFn(sqlite3_context*ctx,int argc,sqlite3_value**argv){
  const char *value;
  if(argc!=1 || sqlite3_value_type(argv[0])!=SQLITE_INTEGER || sqlite3_value_int(argv[0])<0){
    sqlite3_result_error(ctx,"nonnegative compile option index required",-1);return;
  }
  value=sqlite3_compileoption_get(sqlite3_value_int(argv[0]));
  if(value)sqlite3_result_text(ctx,value,-1,SQLITE_TRANSIENT);
  else sqlite3_result_null(ctx);
}

int sqlite3_svsimdisk_init(sqlite3 *db,char **pzErrMsg,const sqlite3_api_routines *pApi){
  int rc; SQLITE_EXTENSION_INIT2(pApi);
  if(sqlite3_libversion_number()<3030000){
    if(pzErrMsg)*pzErrMsg=sqlite3_mprintf("svsim budget VFS requires SQLite >= 3.30 (SQLITE_DIRECTONLY)");
    return SQLITE_ERROR;
  }
  gParent=sqlite3_vfs_find(0); if(!gParent)return SQLITE_ERROR;
  if(!sqlite3_vfs_find(VFS_NAME)){
    memset(&gVfs,0,sizeof(gVfs)); gVfs.iVersion=2;gVfs.szOsFile=(int)(sizeof(BudgetFile)+gParent->szOsFile+8);gVfs.mxPathname=gParent->mxPathname;gVfs.zName=VFS_NAME;gVfs.pAppData=gParent;
    gVfs.xOpen=vOpen;gVfs.xDelete=vDelete;gVfs.xAccess=vAccess;gVfs.xFullPathname=vFull;gVfs.xDlOpen=vDlOpen;gVfs.xDlError=vDlError;gVfs.xDlSym=vDlSym;gVfs.xDlClose=vDlClose;gVfs.xRandomness=vRandom;gVfs.xSleep=vSleep;gVfs.xCurrentTime=vTime;gVfs.xGetLastError=vLast;gVfs.xCurrentTimeInt64=vTime64;
    gMutex=sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_APP1);
    rc=sqlite3_vfs_register(&gVfs,0); if(rc!=SQLITE_OK)return rc;
  }
  rc=sqlite3_create_function(db,"svsim_disk_guard_config",3,SQLITE_UTF8|SQLITE_DIRECTONLY,0,configFn,0,0);
  if(rc==SQLITE_OK)rc=sqlite3_create_function(db,"svsim_disk_guard_status",0,SQLITE_UTF8|SQLITE_DIRECTONLY,0,statusFn,0,0);
  if(rc==SQLITE_OK)rc=sqlite3_create_function(db,"svsim_disk_guard_prepare",3,SQLITE_UTF8|SQLITE_DIRECTONLY,0,prepareFn,0,0);
  if(rc==SQLITE_OK)rc=sqlite3_create_function(db,"svsim_disk_guard_unprepare",1,SQLITE_UTF8|SQLITE_DIRECTONLY,0,unprepareFn,0,0);
  if(rc==SQLITE_OK)rc=sqlite3_create_function(db,"svsim_disk_guard_identity",1,SQLITE_UTF8|SQLITE_DIRECTONLY,0,identityFn,0,0);
  if(rc==SQLITE_OK)rc=sqlite3_create_function(db,"svsim_disk_guard_compile_option",1,SQLITE_UTF8|SQLITE_DIRECTONLY,0,compileOptionFn,0,0);
#ifdef SVSIM_TEST_FAULTS
  if(rc==SQLITE_OK)rc=sqlite3_create_function(db,"svsim_test_fault_arm",4,SQLITE_UTF8|SQLITE_DIRECTONLY,0,faultArmFn,0,0);
  if(rc==SQLITE_OK)rc=sqlite3_create_function(db,"svsim_test_fault_disarm",0,SQLITE_UTF8|SQLITE_DIRECTONLY,0,faultDisarmFn,0,0);
  if(rc==SQLITE_OK)rc=sqlite3_create_function(db,"svsim_test_power_arm",2,SQLITE_UTF8|SQLITE_DIRECTONLY,0,powerArmFn,0,0);
  if(rc==SQLITE_OK)rc=sqlite3_create_function(db,"svsim_test_power_disarm",0,SQLITE_UTF8|SQLITE_DIRECTONLY,0,powerDisarmFn,0,0);
#endif
  if(rc==SQLITE_OK)rc=sqlite3_set_authorizer(db,guardAuthorizer,0);
  if(rc==SQLITE_OK)sqlite3_db_config(db,SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION,0,0);
#ifdef SQLITE_OK_LOAD_PERMANENTLY
  if(rc==SQLITE_OK) return SQLITE_OK_LOAD_PERMANENTLY;
#endif
  return rc;
}
