#!/usr/bin/perl -w

# Ubuntu packes used 
#    libconfig-simple-perl
#    libjson-perl 
#    libdata-dumper-concise-perl
#    liblwp-protocol-https-perl
#
# This script requires a group named admins
#
#     openstack group create admins
#
#    openstack group add user admins admin 
#    openstack role add --group admins --domain snic admin
#    openstack role add --group admins --domain default admin
#    openstack role add --group admins --project admin admin
#
# Version 1.25

use Config::Simple;
use Data::Dumper;
use FileHandle;

unless(defined($ARGV[0])) {
    print "Usage: $0 config_file\n";
    exit 1;
}
unless(-r $ARGV[0]) {
    print "Config file '$ARGV[0]' is NOT readable by effective uid/gid\n";
    exit 1;
}

our $cfg = new Config::Simple($ARGV[0]);

# Check config
for my $key (qw/os_username os_password os_auth_url supr_username supr_password supr_base_url supr_centre_id supr_ssc_resource supr_local_resource log_level log_file state_file ext_net ext_dns os_local_domain int_cidr int_gw/) {
    unless(defined($cfg->param($key))) {
        die("$key missing in config")
    }
}

Logger::entry(1,"SSC SUPR Sync job started");

my $supr = new SUPR(username => $cfg->param("supr_username"), 
                    password => $cfg->param("supr_password"), 
                    base_url => $cfg->param("supr_base_url"));
our $os = new OpenStack(os_username => $cfg->param("os_username"), 
                        os_password => $cfg->param("os_password"),
                        os_auth_url => $cfg->param("os_auth_url"));

# Prepare some stats
my %stat = ();
$stat{'active_admins'} = 0; 
$stat{'pending_admins'} = 0;
$stat{'disabled_admins'} = 0;
$stat{'supr_person_update'} = 0;
$stat{'user_updated'} = 0;
$stat{'user_created'} = 0;
$stat{'user_disabled'} = 0;
$stat{'user_enabled'} = 0;
$stat{'project_members'} = 0;
$stat{'project_update'} = 0;
$stat{'rolls_added'} = 0;
$stat{'rolls_removed'} = 0;
$stat{'net_added'} = 0;
$stat{'subnet_added'} = 0;
$stat{'router_added'} = 0;

my ($users_last_fetched,$projects_last_fetch_time)=&getState();
$projects_last_fetch_time =~ /(\d{4}-\d{2}-\d{2})/;
my $projects_last_fetch_date = $1;

# Make sure admins group exist in Keystone
my $admins_group_id = ${$os->run('group','show','admins','-f' => 'json')}{'id'};
my $admin_user_id = ${$os->run('user','show','admin','-f' => 'json')}{'id'};

# Fetch external network from Keystone
my $enet = $os->run('network','show',$cfg->param("ext_net"),'-f' => 'json');

unless(defined($enet) || $enet->{'id'}) {
    my $msg = "Can not fetch id of external network: ".$cfg->param("ext_net")." from Keystone";
    Logger::entry(3,$msg);
    die($msg);
}

print "Results from SSC SUPR Sync job (".localtime(time)."):\n\n";


my $admins = $supr->get('group/127/','full_person_data' => 1);

my %osAdmins = map { $_->{'Name'} => 1 } @{$os->run('user','list','-f' => 'json','--group' => 'admins')};


print "\t".(defined($admins) && defined(scalar(@{$admins->{members}}))?(scalar @{$admins->{members}}):0)." admins fetched from SUPR\n";

if(defined($admins) && scalar @{$admins->{members}}) {
    my %suprAdmins = map { "sa".$_->{'id'} => 1 } @{$admins->{members}};
    my %approved_admins = map { $_ => 1 } $cfg->param("approved_admins");
    for my $a (@{$admins->{members}}) {
        if( $approved_admins{$a->{'id'}} ) {
            Logger::entry(1,"Admin ".$a->{'id'}." ($a->{'first_name'} $a->{'last_name'}) found in the approved admin list");
            unless( $osAdmins{"sa$a->{'id'}"} ) {
                Logger::entry(1,"Admin user created with username sa$a->{'id'}");
                # Drop !ascii chars in description.
                my $description = "Admin $a->{'first_name'} $a->{'last_name'}";
                $description =~ s/[^[:ascii:]]//g;
                $os->run('user','create',
                         "sa$a->{'id'}",
                         '-f' => 'json',
                         '--description' => $description,
                         '--email' => $a->{'email'},
                         '--or-show' => undef);
                $os->run('group','add user','admins',"sa$a->{'id'}",undef);
                $os->run('user','set',"sa$a->{'id'}","--enable" => undef);
            }
            $stat{'active_admins'}++;
        } else {
            Logger::entry(1,"Admin ".$a->{'id'}." ($a->{'first_name'} $a->{'last_name'}) is NOT in the approved admin list");
            $stat{'pending_admins'}++;
            next;
        }
    }
    # Diable all other admins in group
    for(keys(%osAdmins)) {
        # Ignora all users we did not create
        next unless( $_=~ /^sa\d+/ );

        unless($suprAdmins{$_}) {
            $os->run('group','remove user','admins',$_);
            $os->run('user','set',$_,"--disable");
            $stat{'disabled_admins'}++;
        }
    }
}

my %osUsers = map { $_->{'Name'} => 1 } @{$os->run('user','list','-f' => 'json','--domain' => 'snic')};

my $users = $supr->get('person/search/', 
                       'modified_since' => $users_last_fetched,
                       'centre_person_id_present' => 1);

my $users_fetch_time=$users->{'began'};

print "\t".(defined($users) && defined(scalar(@{$users->{matches}}))?(scalar @{$users->{matches}}):0)." modified users fetched from SUPR (>= $users_last_fetched)\n";

if(defined($users) && scalar @{$users->{matches}}) {
    for my $u (@{$users->{matches}}) {
        my ($description, $status, $user, $update);

        # Drop !ascii chars in description.
        $description = "$u->{'first_name'} $u->{'last_name'}";
        $description =~ s/[^[:ascii:]]//g;

        $status = '--enable';
                if ($u->{'user_agreement_version'} eq '') {
            $stat{'user_disabled'}++;
            Logger::entry(1,"User ".$u->{'centre_person_id'}." ($u->{'first_name'} $u->{'last_name'}) disabled, no user agreement");
            $status = '--disable';
                }

        unless($osUsers{$u->{'centre_person_id'}}) {
            # Create user;
            Logger::entry(1,"Creating user ".$u->{'centre_person_id'}." in Keystone");
            $stat{'user_created'}++;
            $user = $os->run('user','create',
                             $u->{'centre_person_id'},
                             '--domain' => $os->{'domain'},
                             '-f' => 'json',
                             '--description' => $description,
                             '--email' => $u->{'email'},
                             $status => undef,
                             '--or-show' => undef);
            $osUsers{$u->{'centre_person_id'}}++;
        } else {
            $user = $os->run('user','show',
                             $u->{'centre_person_id'},
                             '--domain' => $os->{'domain'},
                             '-f' => 'json');
        }
        $update = '';
        $update .= "description = $description, " if($description ne $user->{'description'});
        $update .= "email = $u->{'email'}, " if($u->{'email'} ne $user->{'email'});
        if ($u->{'user_agreement_version'} ne '' && $user->{'enabled'} ne 1) {
            $update .= "enabled = true, ";
            $stat{'user_enabled'}++;
        }
        $update =~ s/, $//;

        if($update ne '') {
            $stat{'user_updated'}++;
            Logger::entry(1,"Updating user ".$u->{'centre_person_id'}." ($update) in Keystone");
            $os->run('user','set',
                     $user->{'id'},
                     '--description' => "$u->{'first_name'} $u->{'last_name'}",
                     $status => undef,
                     '--email' => $u->{'email'});
        }
    }    
}

my (undef,undef,undef,$day,$mon,$year) = localtime();
my $date_today = sprintf("%02d-%02d-%02d",(1900+$year),(1+$mon),$day);

if($projects_last_fetch_date ne $date_today) {
        Logger::entry(1,"New day, fetch all projects with start_date 'ge' $projects_last_fetch_date");
	# Fetch all projects that starts today
        &manage_projects($supr->get('project/search/', 
                          'start_date_ge' => $projects_last_fetch_date,
                          'managed_in_supr' => 1,
                          'full_person_data' => 1,
                          'resource_centre_id' => $cfg->param('supr_centre_id'),
                         ));
} 

# Fetch all updated projects sice last run from SUPR
my $projects_fetch_time = &manage_projects($supr->get('project/search/', 
                            'modified_since' => $projects_last_fetch_time,
                            'managed_in_supr' => 1,
                            'full_person_data' => 1,
                            'resource_centre_id' => $cfg->param('supr_centre_id'),
                           ));

print <<EOF;
\t$stat{'active_admins'} active admins
\t$stat{'pending_admins'} admins pending approval
\t$stat{'disabled_admins'} admins disabled
\t$stat{'project_members'} members in active projects
\t$stat{'supr_person_update'} centre_person_ids updated in SUPR
\t$stat{'user_created'} users added in Keystone
\t$stat{'user_updated'} users updated in Keystone
\t$stat{'user_disabled'} users disabled, they have not accepted the user agreement
\t$stat{'user_enabled'} users enabled due to accepted user agreement 
\t$stat{'project_update'} projects added or updated in Keystone
\t$stat{'rolls_removed'} roll assignments removed in Keystone
\t$stat{'rolls_added'} roll assignments added in Keystone
\t$stat{'net_added'} networks added in Keystone
\t$stat{'subnet_added'} subnets added in Keystone
\t$stat{'router_added'} routers added in Keystone

EOF


Logger::entry(1,"SSC SUPR Sync job finished");

&setState($users_fetch_time,$projects_fetch_time);

exit 0;

#--- Subs and Packges ---#

sub manage_projects {
    my $projects = shift;

    print "\t".(defined($projects) && defined(scalar(@{$projects->{matches}}))?(scalar @{$projects->{matches}}):0)." projects fetched from SUPR (>= $projects_last_fetch_time)\n";

    
    # Update or create projects in OpenStack
    if(defined($projects) && scalar @{$projects->{matches}}) {
        for my $p (@{$projects->{matches}}) {
    
            my %suprMembersRaw;
            my %suprMembers;
            my $update = 0;
	    my $projectDomain = $os->{'domain'};
	    my $is_ssc = 0;
	    my $is_local = 0;
	
            # Is it a SNIC project or a Local?
            # It can be both, but within a region we only allow one of them.
            foreach( @{$p->{'resourceprojects'}} ) {
		$is_ssc = 1 if( $_->{'resource'}->{'name'} eq $cfg->param("supr_ssc_resource")  &&
                                $_->{'resource'}->{'centre'}->{'id'} == $cfg->param('supr_centre_id') );
		$is_local = 1 if( $_->{'resource'}->{'name'} eq $cfg->param("supr_local_resource") && 
                                  $_->{'resource'}->{'centre'}->{'id'} == $cfg->param('supr_centre_id'));
		if($is_local) {
			$projectDomain = $cfg->param("os_local_domain");
		}
	    }
	    next if($is_local == 0 && $is_ssc == 0);
    
            # Add PI as member
            $suprMembersRaw{"s".$p->{'pi'}->{id}} = $p->{'pi'};
    
            # Fetch add proxy as a member
            if(defined($p->{'proxy'}->{id})) {
                $suprMembersRaw{"s".$p->{'proxy'}->{id}} = $p->{'proxy'}; 
            }
    
            # Fetch project members
            for my $m (@{$p->{members}}) {
                $suprMembersRaw{"s".$m->{id}} = $m;
            }
    
            # Get userneme of pi
            my $pi_id = $p->{'pi'}->{'centre_person_id'}?$p->{'pi'}->{'centre_person_id'}:"s".$p->{'pi'}->{id};
   	     
            my $continuation = (defined($p->{'continuation_name'}) && $p->{'continuation_name'} =~ /^[A-Za-z0-9]+/)?1:0;
    
            if($p->{'start_date'} gt $date_today) {
                Logger::entry(1,"Will not add project '$p->{'name'}' until start_date $p->{'start_date'}");
                next;
            } elsif ($p->{'end_date'} lt $date_today) {
                Logger::entry(1,"Project '$p->{'name'}' has passed end date $p->{'end_date'} ignoring");
		next;
            }
    
            if($continuation && !defined(getProject($p->{'name'}))) {
                my $op_proj = $os->run('project','show',
                                       $p->{'continuation_name'},
                                       '-f' => 'json');
                if(defined($op_proj)) {
                    if($pi_id ne $op_proj->{'supr_pi'}) {
                        Logger::entry(2,"Project not renamed. PI of '$p->{'continuation_name'}' and '$p->{'name'}' differ.");
                        Logger::entry(2,"Will not create project '$p->{'name'}', please resolve the PI issue manually");
                        next;
                    }
		    my $os_domain = $os->run('domain','show',
			    		     $op_proj->{'domain_id'},
					     '-c' => 'name',
					     '-f' => 'json');
		   
                    # Can not handle domainchange in continuation
                    if($os_domain->{'name'} ne $projectDomain) {
                        Logger::entry(2,"Can not change domain to $projectDomain for $p->{'name'}");
                        Logger::entry(2,"Creating new project, $p->{'continuation_name'} can not be renamed to $p->{'name'}");
                        undef $continuation;
                    } else {
                        &renameProject($p->{'continuation_name'},$p->{'name'});
                        $update=1;
                    }
                } else {
                    Logger::entry(2,"Project $p->{'continuation_name'} does not exist, can not create continuation.");
                    undef $continuation;
                }
            }
    
            # Create project if it does not exist
            my $op = $os->run('project','create',
                              $p->{'name'},
                              '-f' => 'json',
                              '--domain' => $projectDomain,
                              '--property' => "supr_pi=$pi_id",
                              '--description' => $p->{'title'},
                              '--or-show' => undef
                             );
    
            # Jump to next if the project is disabled.
            next unless($op->{'enabled'});
    
            # Update PI field
            if($op->{'supr_pi'} ne $pi_id) {    
                Logger::entry(1,"Updating pi on project $p->{'name'} in Keystone");
                $update=1;
                $os->run('project','set',$op->{'id'},'--property' => "supr_pi=$pi_id");
            }
    
            # Update description if title has changed
            if($op->{'description'} ne $p->{'title'}) {    
                Logger::entry(1,"Updating or creating project $p->{'name'} in Keystone");
                $update=1;
                $os->run('project','set',$op->{'id'},'--description' => $p->{'title'});
            }
                
            $stat{'project_update'}++ if($update);
    
            # get list of users for translation
            my %id2name = map {$_->{'ID'} => $_->{'Name'}} @{
                $os->run('user','list','--domain' => 'snic','-f' => 'json')};
    
            # Get a list of all members in openstack
            my %osMembers = map {$id2name{$_->{'User'}} => 1} @{
                $os->run('role','assignment list',
                         '-f' => 'json',
                         '--project' => $op->{'id'},
                         '--role' => '_member_')};
    
            # Update or create users for the project
            for my $sp (values(%suprMembersRaw)) {
                $stat{'project_members'}++;
    
                if($sp->{'centre_person_id'} eq "") {
                    Logger::entry(1,"Setting centre_person_id to s".$sp->{'id'}." in SUPR");
                    $supr->post("person/$sp->{'id'}/update/", "centre_person_id" => "s".$sp->{'id'});
                    $stat{'supr_person_update'}++;
                }
                
                my $id = $sp->{'centre_person_id'}?$sp->{'centre_person_id'}:"s".$sp->{id};
                $suprMembers{$id}++;
        
                unless( $osUsers{$id} ) {
                    my $status = '--enable';
                    if ($sp->{'user_agreement_version'} eq '') {
                        Logger::entry(1,"User $id ($sp->{'first_name'} $sp->{'last_name'}) disabled, no user agreement");
                        $status = '--disable';
                    }
    
                    $stat{'user_created'}++;    
                    $osUsers{$id}++;
                    # Try to create :)
                    my $user = $os->run('user','create',
                                        $id,
                                        '--domain' => $os->{'domain'},
                                        '-f' => 'json',
                                        '--description' => "$sp->{'first_name'} $sp->{'last_name'}",
                                        '--email' => $sp->{'email'},
                                        $status => undef,
                                        '--or-show' => undef);
                }
            }
    
            my %admins = map {$_->{'User'}?($_->{'User'} => 1):($_->{'Group'} => 1)} @{
                $os->run('role','assignment list',
                         '-f' => 'json',
                         '--project' => $op->{'id'},
                         '--role' => 'admin')};
        
            # Make sure admin is admin of the project
            unless($admins{$admin_user_id}) {
                Logger::entry(1,"Adding admin as admin for $p->{'name'}");
                $os->run('role','add',
                         'admin',
                         '--user' => 'admin',
                         '--project' => $op->{'id'});
            } 
            unless($admins{$admins_group_id}) {
                Logger::entry(1,"Adding group admins as admin for $p->{'name'}");
                $os->run('role','add',
                         'admin',
                         '--group' => 'admins',
                         '--project' => $op->{'id'});        
            }
    
    
            # Remove roles
            for my $osMember (keys %osMembers) {
                unless($suprMembers{$osMember}) {
                    $stat{'rolls_removed'}++;
                    Logger::entry(1,"Removing $osMember as _member_ in $p->{'name'}");
                    removeRoleMember($osMember,'_member_',$op->{'id'});
                }
            }
    
            # Add roles
            for my $suprMember (keys %suprMembers) {
                unless($osMembers{$suprMember}) {
                    $stat{'rolls_added'}++;
                    addRoleMember($suprMember,'_member_',$op->{'id'});
                } else {
                    Logger::entry(4,"$suprMember is already _member_ in $p->{'name'}");
                }
            }
    
            # Set up resource names
            my $router_name = "$p->{'name'} IPv4 Router";
            my $network_name = "$p->{'name'} Internal IPv4 Network";
            my $subnet_name = "$p->{'name'} Internal IPv4 Subnet";
            my $old_router_name = $continuation?"$p->{'continuation_name'} IPv4 Router":'';
            my $old_network_name = $continuation?"$p->{'continuation_name'} Internal IPv4 Network":'';
            my $old_subnet_name = $continuation?"$p->{'continuation_name'} Internal IPv4 Subnet":'';
    
            # Fetch router first, if there is a router then the networks must exist :)
            unless(defined(getResource('router',"$p->{'name'} IPv4 Router"))) {
    
                # Fetch project network
                unless(defined(getResource('network',$network_name))) {
                    if($continuation && defined(getResource('network', $old_network_name))) {
                        renameResource('network', $old_network_name, $network_name);
                    } else {
                        # Create network
                        Logger::entry(1,"Creating network '$network_name' for project $p->{'name'} in Keystone");
                        $os->run('network','create',
                                 $network_name,
                                 '--project' => $op->{'id'},
                                 '-f' => 'json');
                        $stat{'net_added'}++;
                    }
                }
    
                # Fetch project subnet
                unless(defined(getResource('subnet', $subnet_name))) {
                    if($continuation && defined(getResource('subnet', $old_subnet_name))) {
                        renameResource('subnet',$old_subnet_name, $subnet_name);
                    } else {
                        # Create subnet
                        Logger::entry(1,"Creating subnet '$subnet_name' for project $p->{'name'} in Keystone");
                        $os->run('subnet','create',
                                 $subnet_name,
                                 '--network' =>  $network_name,
                                 '--dns-nameserver' => $cfg->param("ext_dns"),
                                 '--gateway' => $cfg->param("int_gw"),
                                 '--subnet-range' => $cfg->param("int_cidr"),
                                 '--project' => $op->{'id'},
                                 '-f' => 'json');
                        $stat{'subnet_added'}++;
                    }
                }
    
                if($continuation && defined(getResource('router', $old_router_name))) {
    		renameResource('router',$old_router_name, $router_name);
                } else {
                    # Create router
                    Logger::entry(1,"Creating router '$router_name' for project $p->{'name'} in Keystone");
                    $os->run('router','create',$router_name,'--project' => $op->{'id'},'-f' => 'json');
		    $os->run('router','set',$router_name,'--external-gateway',$enet->{'id'});
                    $os->run('router','add subnet',$router_name, $subnet_name);
                    $stat{'router_added'}++;
                }
            }            
        }
    }
    return $projects->{'began'};
}

sub addRoleMember {
    my ($u,$r,$p) = @_;
    Logger::entry(1,"Adding $u as $r in $p");
    $os->run('role','add',$r,'--user' => $u, '--project' => $p);
}

sub removeRoleMember {
    my ($u,$r,$p) = @_;
    Logger::entry(1,"Removing $u as $r in $p");
    $os->run('role','remove',$r,'--user' => $u, '--project' => $p);
}

sub getResource {
    my ($r,$n) = @_;
    return $os->run($r,'show',$n,'-f' => 'json');
}

sub getProject {
    my ($p) = @_; 
    return $os->run('project','show',$p,'-f' => 'json','--domain' => $os->{'domain'});
}


sub renameProject {
    my ($old_name, $new_name) = @_;

    # Rename old project to new project
    Logger::entry(1,"Renaming project '$old_name' to '$new_name'");
    $os->run('project','set',
             $old_name,
             '--name' => $new_name,
             '--domain' => $os->{'domain'},
             '--property' => "supr_continuation=$old_name");
}
sub renameResource {
    my ($r, $old_name, $new_name) = @_;
    Logger::entry(1,"Renaming $r '$old_name' to '$new_name'");
    $os->run($r,'set',$old_name,'--name' => $new_name);
}

sub getState {
    my ($uf, $pf);
    open(STATE, "<", $cfg->param("state_file")) 
        || Logger::entry(2,"Unable to open ".$cfg->param("state_file")." for read: $!");
    if(defined(fileno(STATE))) {
        while (<STATE>) {
            chomp;
            if(/^user:(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/) {
                $uf = $1;
            } elsif(/^project:(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/) {
                $pf = $1;    
            }
        }
        close(STATE);
    }
    return(
        (!defined($uf) || $uf !~ m/\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/)?'2017-01-01 00:00:00':$uf,
        (!defined($pf) || $pf !~ m/\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/)?'2017-01-01 00:00:00':$pf);
}

sub setState {
    my ($uf,$pf)=@_;
    open(STATE, ">", $cfg->param("state_file"))
        || Logger::entry(2,"Unable to open ".$cfg->param("state_file")." for write: $!");
    if(defined(fileno(STATE))) {
        print STATE "user:$uf\n";
        print STATE "project:$pf\n";
        close(STATE);
    }
}

package OpenStack;

use JSON;
use Data::Dumper;
use IPC::Open3;
use IO::Select;

sub new {
    my $type = shift;
    my %params = @_;
    my $self = {
        'domain' => 'snic',
        'os_cmd' => '/usr/local/bin/openstack',
        'cmd_args' => '',
        'os_username' => undef,
        'os_password' => undef,
        'os_auth_url' => undef,
    };
    $self->{'cmd_args'} .= "--os-identity-api-version 3 ";
    $self->{'cmd_args'} .= "--os-project-domain-name default ";
    $self->{'cmd_args'} .= " --os-user-domain-name default ";
    $self->{'cmd_args'} .= "--os-tenant-name admin ";
#    $self->{'cmd_args'} .= "--os-project-name admin ";

    for my $key (qw/os_username os_password os_cmd os_auth_url/) {
        $self->{$key} = $params{$key} if defined $params{$key};
    }

    $self->{'cmd_args'} .= "--os-username $self->{'os_username'} ";
    $self->{'cmd_args'} .= "--os-password $self->{'os_password'} ";
    $self->{'cmd_args'} .= "--os-auth-url $self->{'os_auth_url'} ";

    return bless $self, $type;
}

sub run {
    my $self = shift;
    my $target = shift;
    my $action = shift;
    my $object = '';

    if($action eq 'add subnet') {
        # add subnet has two objects
        $object = quotemeta(shift);
        $object .= " ".quotemeta(shift);
    } elsif ($action eq 'list' || $action eq 'assignment list') {
        # list commands do not have objects
        $object = '';
    } else {
        # Other commands have one object
        $object = quotemeta(shift);
    }


    my $command = '';

    my(%params) = @_;
    my $qparam = '';

    for (keys %params) {
        $qparam .= $_.' ';
        if(defined($params{$_})) {
            if($params{$_} eq '') {
                $qparam .= '"" ';
            } else {
                $qparam .= quotemeta($params{$_}).' ';
            }
        }
    }

    $command = "$self->{os_cmd} $target $action $object $qparam";
    
    # Drop anything non ascii
    $command =~ s/[^[:ascii:]]//g;
    Logger::entry(4,$command);

    my $stdout = '';
    my $stderr = '';
    my $pid = open3( \*P_STDIN, \*P_STDOUT, \*P_STDERR, "$command $self->{'cmd_args'}" ) || warn $!;
    my $select = IO::Select->new(\*P_STDOUT, \*P_STDERR);

    close P_STDIN; # Close the command's STDIN

    while (my @ready = $select->can_read(20)) {
	    foreach my $handle (@ready) {
		    if (sysread($handle, my $buf, 4096)) {
			    if ($handle == \*P_STDOUT) {
				    $stdout .= $buf;
			    }
			    else {
				    $stderr .= $buf;
			    }
		    }
		    else {
			    # EOF or error
			    $select->remove($handle);
		    }
	    }
    }

    if ($select->count) {
	    print "Timed out\n";
	    kill('TERM', $pid);
    }
    #    while (1) {
    #        if ( not eof P_STDOUT ) {
    #            $stdout .= my $out = <P_STDOUT>;
    #        }
    #        if ( not eof P_STDERR ) {
    #            my $err = <P_STDERR>;
    #       
    #            $stderr .= $err;
    #        }
    #        last if eof(P_STDOUT) && eof(P_STDERR);
    #    }
        
    waitpid( $pid, 0 ) || die $!;

    if($stderr =~ /^ResourceNotFound:/) {
        return undef;
    } elsif($stderr =~ /^No Router found for /) {
        return undef;
    } elsif($stderr =~ /No Network found for /) {
        return undef;
    } elsif($stderr =~ /No Subnet found for /) {
        return undef;
    } elsif($stderr =~ /^No project with a name or ID of/) {
        return undef;
    }

    if($stderr ne '') {
        Logger::entry(3,$stderr);
    }
    if($?) {
        Logger::entry(4,$stderr);
        die("CMD: $command\nSTDERR: $stderr");
    }
    
    return undef unless($stdout);
        my $r = eval {
                from_json($stdout, {utf8 => 1});
        };
        return $r if defined $r;
        Logger::entry(4,Dumper("action: $action"));
        Logger::entry(4,Dumper("action: $action"));
        Logger::entry(4,Dumper($stdout));
        return;
} 

package Logger;

sub entry {
    my $level = shift;
    my $event = shift;

    return if($level > $cfg->param("log_level"));

    my @logLevel = ('NONE','INFO','WARN','ERROR','DEBUG');
    
    my $entry = localtime(time)."  $logLevel[$level]\t[$$]\t$event\n";

    if(!defined($cfg->param('log_file')) || $cfg->param('log_file') eq '') {
        die "Logfile not defined in config";
    }

    if(defined($cfg->param('log_file')) && $cfg->param("log_file") ne '') {
        open(L, ">>".$cfg->param('log_file')) || 
                die "Unable to open logfile ".$cfg->param('log_file').": $!";
	autoflush L 1;
        print L $entry;
        close(L);
    }

#    print $entry;
}

package SUPR;

use JSON;
use LWP;
use Data::Dumper;

sub new {
    my $type = shift;
    my %params = @_;
    my $self = {
        'base_url' => 'https://disposer.c3se.chalmers.se/supr-test/api',
        'username' => undef,
        'password' => undef,
        'browser'  => MyUserAgent->new,
        'agent'    => 'SSC/1.0',
    };
    for my $key (qw/base_url username password agent/) {
        $self->{$key} = $params{$key} if defined $params{$key};
    }
    $self->{'browser'}->set_basic_auth($self->{'username'},$self->{'password'});
    $self->{'browser'}->agent($self->{agent});
    return bless $self, $type;
}

sub get {
    my($self,$action,%params) = @_;
    my $uri = URI->new(join("/",$self->{'base_url'},$action));
    $uri->query_form(%params);
    my $req = HTTP::Request->new(GET => $uri);
    # LWP failes on chunked :-( Force 1.0
     $req->protocol('HTTP/1.0');
    my $response = $self->{'browser'}->request($req);

    Logger::entry(2,"SUPR ".$response->{'_content'}) unless($response->is_success);
        Logger::entry(4,Dumper($response)) unless($response->is_success);
    die() unless($response->is_success);

    #return if $response->content eq '';
    my $r = eval {
        from_json($response->content, {utf8 => 1});
    };
    return $r if defined $r;

    Logger::entry(4,Dumper("action: $action"));
    Logger::entry(4,Dumper("action: $action"));
    Logger::entry(4,Dumper($response));

    die("SUPR: $uri\n".Dumper($response));
}

sub post {
    my($self,$action,%params) = @_;
    my $uri = URI->new(join("/",$self->{'base_url'},$action));
    my $req = HTTP::Request->new(POST => $uri);

    $req->content_type('application/x-www-form-urlencoded');
    $req->content(to_json(\%params, {utf8 => 1}));
    $req->protocol('HTTP/1.0');
    my $response = $self->{'browser'}->request($req);
    Logger::entry(2,"SUPR ".$response->{'_content'}) unless($response->is_success);
        Logger::entry(4,Dumper($response)) unless($response->is_success);
    return undef unless $response->is_success;
    my $r = eval {
        from_json($response->content, {utf8 => 1});
    };
    return $r if defined $r;
    Logger::entry(4,Dumper("action: $action"));
    Logger::entry(4,Dumper("action: $action"));
    Logger::entry(4,Dumper($response));
    return;
}

package MyUserAgent;

use base 'LWP::UserAgent';

sub set_basic_auth {
    my($self,$username,$password) = @_;
    $self->{'_my_username'} = $username;
    $self->{'_my_password'} = $password;
    return $self;
}

sub get_basic_credentials {
    my($self) = @_;
    return $self->{'_my_username'},$self->{'_my_password'};
}

