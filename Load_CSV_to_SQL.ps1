function truncate-stageTable
{
    param($destServer,$destDb,$destTbl)
		$connectionString = "Data Source=$destServer;Integrated Security=true;Initial Catalog=$destdb;"
		$conn = New-Object System.Data.SqlClient.SqlConnection
		$conn.ConnectionString = $connectionString 
		$conn.open()
		$cmd = New-Object System.Data.SqlClient.SqlCommand
		$cmd.connection = $conn
		$cmd.commandtext = "TRUNCATE TABLE $destTbl"
		If($cmd.executenonquery() -eq -1) { Write-Host "Succesfully cleared table" }
		$conn.close()    
}

function Load-stage
{
	param($filepath,$csvdelimiter,$destServer,$destDb,$destTbl,$colCount)
		 
		[void][Reflection.Assembly]::LoadWithPartialName("System.Data") 
		[void][Reflection.Assembly]::LoadWithPartialName("System.Data.SqlClient") 
		 $batchsize=50000
		 # Build the sqlbulkcopy connection, and set the timeout to infinite 
		 $connectionstring = "Data Source=$destServer;Integrated Security=true;Initial Catalog=$destDb;"
		 
		 $bulkcopy = New-Object Data.SqlClient.SqlBulkCopy($connectionstring, [System.Data.SqlClient.SqlBulkCopyOptions]::TableLock) 
		 $bulkcopy.DestinationTableName = $destTbl 
		 $bulkcopy.bulkcopyTimeout = 0 
		 $bulkcopy.batchsize = $batchsize 
		   
		 # Create the datatable, and autogenerate the columns. 
		 $datatable = New-Object System.Data.DataTable 
		   
		 # Open the text file from disk 
		 $reader = New-Object System.IO.StreamReader($filepath) 
		 $columns = (Get-Content $filepath -First 1).Split($csvdelimiter,$colCount) 
		 if ($FirstRowColumnNames -eq $true) { $null = $reader.readLine() } 
		 #Write-Host "Column count --------------" $columns.count   
		 foreach ($column in $columns) {
			#write-host $column 
			$null = $datatable.Columns.Add() 
		 } 
		 $inc = $columns.count
		 #Write-Host "datatable.Columns --------"$datatable.Columns.count
		 #Write-Host "inc = $inc : colCount : $colCount"
		 while ($inc -lt $colCount)
			{
				$null = $datatable.Columns.Add()
				$inc=$inc+1
				#Write-Host "datatable.Columns --------"$datatable.Columns.count
			}
		 # Read in the data, line by line, not column by column 
		 while (($line = $reader.ReadLine()) -ne $null) { 
			$tmpVar=$line.Split("`"")
			#Write-Host $tmpVar
			$tmpVar1=""
			if ( $tmpVar.count -gt 1 -and $tmpVar.count % 2 -ne 0) {
				$inc=0
				while ($inc -lt $tmpVar.count)
					{
					if ($inc%2 -ne 0){$tmpVar1= $tmpVar1 + ($tmpVar[$inc] -replace "$csvdelimiter","-")} else {$tmpVar1= $tmpVar1 +$tmpVar[$inc]}
					$inc=$inc+1
					}
			} 
			else {$tmpVar1= $line} 
			#write-host $colCount
			#Write-Host "tmpVar1 Array ----------- "+ $tmpVar1.count
			$tmpArr=$tmpVar1 -Split($csvdelimiter,$colCount)
			#Write-Host $tmpArr 
			#Write-Host "tmpArr Array ----------- " + $tmpArr.count			
			$null = $datatable.Rows.Add($tmpArr) 
			# Import and empty the datatable before it starts taking up too much RAM, but  
			# after it has enough rows to make the import efficient. 
			$i++; 
			if (($i % $batchsize) -eq 0) {  
				$bulkcopy.WriteToServer($datatable)  
				Write-Host "$i rows have been inserted to $destTbl in $($elapsed.Elapsed.ToString())." 
				$datatable.Clear()  
			}  
		 }  
		   
		 # Add in all the remaining rows since the last clear 
		 if($datatable.Rows.Count -gt 0) { 
			$bulkcopy.WriteToServer($datatable) 
			$datatable.Clear() 
		 }
		 # Clean Up 
		 $reader.Close(); 
		 $reader.Dispose(); 
		 $bulkcopy.Close(); 
		 $bulkcopy.Dispose(); 
		 $datatable.Dispose(); 
  
}

function execSP-PushStagetoRaw
{
    param($destServer,$destDb,$filepath)
		$connectionString = "Data Source=$destServer;Integrated Security=true;Initial Catalog=$destdb;"
		$conn = New-Object System.Data.SqlClient.SqlConnection
		$conn.ConnectionString = $connectionString 
		$conn.open()
		$cmd = New-Object System.Data.SqlClient.SqlCommand
		$cmd.connection = $conn
		$cmd.commandtext = "exec sp_LH_Sales_load `'$filepath`'" 
		If($cmd.executenonquery() -eq -1) { Write-Host "Procedure executed"  }
		echo "'$fp' loaded"
		Move-Item -Path $filepath  "\\WSQGSC1003PRD\FTP-Root\CoP_files\Australia\Loaded" -Force
		$conn.close()
}

Write-Host "Script started..." 
$elapsed = [System.Diagnostics.Stopwatch]::StartNew()  
# Database variables 
$sqlserver = "WSQGSC1003PRD" 
$database = "CoP_Channel_Visibility" 
$table = "[stage].[Sales_LH]" 

# CSV variables 
$file = "C:\Users\sesa507938\Desktop\Aug_data\Data used for 201808\LH sales\Sales Out Aug 2018.csv" 
$path="\\WSQGSC1003PRD\FTP-Root\CoP_files\Australia\L&H"
$csvdelimiter = "," 
$FirstRowColumnNames = $True 
$colCount=14
foreach ($file in (get-childitem -path "\\WSQGSC1003PRD\FTP-Root\CoP_files\Australia\L&H" -Filter Sales*Out*.csv -Recurse | where {!$_.PSIsContainer} | select-object FullName))
{
  $fp=$file.FullName
  Write-Host $fp
  truncate-stageTable $sqlserver $database $table
  Write-Host $fp
  Load-stage $fp $csvdelimiter $sqlserver $database $table $colCount
  execSP-PushStagetoRaw $sqlserver $database $fp
}
# Sometimes the Garbage Collector takes too long to clear the huge datatable. 
[System.GC]::Collect() 
Write-Host "Script Completed..."
