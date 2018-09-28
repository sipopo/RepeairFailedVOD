﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.IO;
using System.Diagnostics;
using System.Threading;

namespace RepeairFailedVOD
{
    class Program
    {
        
        static void CheckJobExecution(object JobID)
        {
            // Check running Job
            try
            {
                Guid JobGuid = Guid.Parse("00000000-0000-0000-0000-000000000000");
                if (Guid.TryParse(JobID.ToString(), out JobGuid))
                { 
                    OssVodBranchWS VodBranch = new OssVodBranchWS();
                    VodBranch.Credentials = new NetworkCredential("IPTVServices", "P23R@vor", "BRMSK");
                    VodBranch.Url = "http://78.107.199.132/ossVodBranchWS/branch.asmx";
                    
                    AssetInfo AssetInfo = new AssetInfo();

                    do
                    {
                        Thread.Sleep(180000);
                        AssetInfo = VodBranch.GetAssetInfoByJobId(JobGuid);
                        Console.WriteLine(AssetInfo.ProviderAssetId + ": Checking job " + JobGuid + ", status : " + AssetInfo.Status + " cluster :" + AssetInfo.ClusterName);
                        Log(AssetInfo.ProviderAssetId + ": Checking job " + JobGuid + " status : " + AssetInfo.Status + " server :" + AssetInfo.ServerName + "cluster :" + AssetInfo.ClusterName );
                        
                    } while ((AssetInfo.Status == JobStatusCode.InProgress) || (AssetInfo.Status == JobStatusCode.Ready));
                    // Do something more
                    // Here we need check contract in MDS and remove it. if exist/                    
                }  
                              
            }
            catch (Exception e)
            {
                Log("Error :" + e.Message);
                Console.WriteLine("Error :" + e.Message);
            }            
        }

            static void Main(string[] args)
        {
            try
            {
                Log("Start Program");

                int MaxAsset = 2;
                Guid nullGuid = Guid.Parse("00000000-0000-0000-0000-000000000000");
                

                OssVodBranchWS VodBranch = new OssVodBranchWS();

                VodBranch.Credentials = new NetworkCredential("IPTVServices", "P23R@vor", "BRMSK");
                VodBranch.Url = "http://78.107.199.132/ossVodBranchWS/branch.asmx";



                Log("Check status of Clusters");
                List<Cluster> BadClusters = new List<Cluster>();                                
                Cluster[] Clusters = VodBranch.GetAllClusterInfo();
                foreach (Cluster cluster in Clusters)
                {
                    //new   
                    VServer[] vservers = VodBranch.GetAllVServerForCluster(cluster.ClusterId);
                    foreach (VServer vserver in vservers)
                    {
                        if (vserver.Status != VServerStatus.Available)
                        {
                            BadClusters.Add(cluster);
                            Log("Cluster " + cluster.Name + " is not good. VServer " + vserver.Name + " has status: " + vserver.Status);
                            break;
                        }
                    }                        
                }



                FailedResourceInformation FailedResourceInformation = VodBranch.GetFailedResourceInformation();

                Log("The count Failed Assets is " + FailedResourceInformation.AssetServerMaps.Count() );
                Console.WriteLine("The count Failed Assets is " + FailedResourceInformation.AssetServerMaps.Count());

                int i = 0;

                foreach (AssetServerMap AssetServerMap in FailedResourceInformation.AssetServerMaps)
                {
                    // Check info about AssetServerMap
                    string ProviderID = AssetServerMap.ProviderId;
                    string ProviderAssetID = AssetServerMap.ProviderAssetId;
                    Guid ClusterID = AssetServerMap.VServerDiskInformation.VServerInformation.ClusterId;
                    Guid JobGuid = new Guid();
                    
                                        
                    AssetInfo[] AssetsInfo = VodBranch.GetAssetServerMap(ProviderID, ProviderAssetID, ClusterID, nullGuid, null);
                    if ( AssetsInfo.Length < 2 )
                    {
                        Log(ProviderAssetID +  ": In cluster " + AssetServerMap.VServerDiskInformation.VServerInformation.ClusterName + " we have only " + AssetsInfo.Length + " asset(s)");
                        Console.WriteLine(ProviderAssetID + ": In cluster " + AssetServerMap.VServerDiskInformation.VServerInformation.ClusterName + " we have only " + AssetsInfo.Length + " asset(s)");

                        bool TryToDo = true;

                        foreach (Cluster BadCluster in BadClusters)
                        {
                            // We want to know the health of Cluster
                            if (AssetServerMap.VServerDiskInformation.VServerInformation.ClusterId == BadCluster.ClusterId) 
                            {
                                TryToDo = false;
                                break;
                            }
                        }

                        if (TryToDo)
                        {
                            // It needed for make a Job

                            JobType jb = new JobType();
                            string BackendName= "";
                            string AssetName = "";                     
                            string SourceLocation = "";

                            Random rnd = new Random();
                            int rndMinutes = rnd.Next(2, 10);
                            DateTime ScheduleTime = DateTime.Now;
                                                        
                            ScheduleTime = ScheduleTime.AddMinutes(rndMinutes);
                            
                            


                            AssetInfo[] JobAssetsInfo = VodBranch.GetClusterAssetJobMapByAssetID(0, 0, true, ProviderAssetID);
                            foreach (AssetInfo JobAssetInfo in JobAssetsInfo)
                            {
                                if (JobAssetInfo.ClusterId == ClusterID)
                                {
                                    Log(ProviderAssetID + ": The Job was " + JobAssetInfo.JobType );
                                    jb = JobAssetInfo.JobType;
                                    BackendName = JobAssetInfo.Backend;
                                    AssetName = JobAssetInfo.AssetName;
                                    SourceLocation = JobAssetInfo.SourceLocation;
                                    break;
                                }
                            }
                     
                            switch (jb)
                            { 
                                case (JobType.Copy):
                                    Console.WriteLine("Copy this Assets");
                                    JobGuid = VodBranch.NewClusterJob(AssetName, ProviderAssetID, ProviderID, ClusterID, BackendName, SourceLocation, jb, ScheduleTime, null, 0, 3);
                                    Log(ProviderAssetID + " : We will try to Copy to " + AssetServerMap.VServerDiskInformation.VServerInformation.ClusterName + " Job guid " + JobGuid);
                                    // After Job ended we need to delete "Bad" price in MDS !!! It is important!!!
                                    //
                                    Thread oThread = new Thread(CheckJobExecution);
                                    object JobID = JobGuid.ToString();

                                    oThread.Start(JobID);

                                    break;
                                case (JobType.Delete):
                                    Console.WriteLine("Delete this Assets");
                                    JobGuid = VodBranch.NewClusterJob(AssetName, ProviderAssetID, ProviderID, ClusterID, BackendName, SourceLocation, jb, ScheduleTime, null, 0, 3);
                                    Log(ProviderAssetID + " : We will Delete this asset. Job guid " + JobGuid + " it will start at " + ScheduleTime.ToString("yyyy-MM-dd HH:mm:ss"));
                                break;
                            }

                            //Console.WriteLine(ProviderAssetID + ": We will try to Deploy on " + AssetServerMap.VServerDiskInformation.VServerInformation.ClusterName);
                        }
                    }


                    i = i + 1;
                    if (i>MaxAsset) { break; }

                    //AssetServerMap.Status --
                    //GetAllVServerForCluster  -- check status servers 
                }


            } catch (Exception e)                
            {
                Log("Error :" + e.Message );
                Console.WriteLine("Error :" + e.Message);

            } finally { 
                Log("End Program");
                Console.WriteLine("End Program");
                //Console.ReadLine();
            }
                
        }

        public static void Log(string logMessage)
        {
            //string path = Properties.Settings.Default.LogPath;
            string path = ".\\";


            string filename = Process.GetCurrentProcess().ProcessName + "_" + DateTime.Today.ToString("yyyy-MM-dd") + ".log";

            try
            {
                using (StreamWriter w = File.AppendText(path + "\\" + filename))
                {
                    w.Write("{0}\t", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                    w.WriteLine(logMessage);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error to write file log " + e.ToString());
            }

        }// end of log
    }
}
